use super::*;

const KEYS: u8 = 4;
const EVENTS: usize = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    Flaky(u32),
    Terminal,
    Outage,
}

fn fate(code: u8) -> Fate {
    match code {
        0..=2 => Fate::Flaky(u32::from(code)),
        3 => Fate::Terminal,
        _ => Fate::Outage,
    }
}

fn script(fates: Vec<Fate>, outage_until: Instant) -> Script {
    Box::new(
        move |ordinal, attempt| match fates[usize::try_from(ordinal).unwrap()] {
            Fate::Flaky(failures) if attempt <= failures => {
                Outcome::Retryable(SinkError("flaky".to_owned()))
            }
            Fate::Terminal => Outcome::Terminal(SinkError("rejected".to_owned())),
            Fate::Outage if Instant::now() < outage_until => {
                Outcome::Retryable(SinkError("outage".to_owned()))
            }
            Fate::Flaky(_) | Fate::Outage => Outcome::Delivered,
        },
    )
}

async fn start_phase(
    store: TestStore,
    keys: &[u8],
    fates: &[Fate],
    outage_until: Instant,
    lanes: usize,
) -> Harness {
    let setup = Setup {
        lanes,
        concurrency: 2,
        park_after: Duration::from_secs(1),
        ..Setup::default()
    };
    let sink = ScriptedSink::new(LIMITS, script(fates.to_vec(), outage_until));
    let mut harness = Harness::start(setup, sink, store).await;
    for (ordinal, key) in keys.iter().enumerate() {
        harness
            .send(u32::try_from(ordinal).unwrap(), &[*key], 10)
            .unwrap();
    }
    harness
}

async fn run_phase(
    store: TestStore,
    keys: &[u8],
    fates: &[Fate],
    outage_until: Instant,
    lanes: usize,
) -> Result<Harness, TestCaseError> {
    let mut harness = start_phase(store, keys, fates, outage_until, lanes).await;
    let last = Lsn(100 + u64::try_from(keys.len() - 1).unwrap());
    prop_assert_eq!(harness.settle(keys.len()).await, last);
    prop_assert_eq!(harness.ack.inflight_events(), 0);
    tokio::time::sleep(Duration::from_secs(120)).await;
    Ok(harness)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn o2_o3_a1_lanes_park_and_drain_under_random_failures_partial_appends_and_restarts(
        keys in proptest::collection::vec(0u8..KEYS, 1..EVENTS),
        codes in proptest::collection::vec(0u8..6, EVENTS),
        store_failures in 0u32..4,
        landed in 0usize..3,
        lanes in 1usize..3,
    ) {
        let fates: Vec<Fate> = codes.iter().copied().map(fate).collect();
        let mut dead_at: Vec<Option<usize>> = vec![None; usize::from(KEYS)];
        for (ordinal, key) in keys.iter().enumerate() {
            let slot = &mut dead_at[usize::from(*key)];
            if slot.is_none() && fates[ordinal] == Fate::Terminal {
                *slot = Some(ordinal);
            }
        }
        let blocked = |ordinal: usize| {
            dead_at[usize::from(keys[ordinal])].is_some_and(|dead| ordinal >= dead)
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let outage_until = Instant::now() + Duration::from_secs(5);
            let store = TestStore::default();
            store.fail_appends.store(store_failures, Ordering::SeqCst);
            store.land_on_failure.store(landed, Ordering::SeqCst);
            let first = run_phase(store, &keys, &fates, outage_until, lanes).await?;
            check_order(&first.sink.batches.lock().unwrap(), &keys)?;
            let mut delivered = first.sink.accepted();
            delivered.sort_unstable();
            let expected: Vec<u32> = (0..keys.len())
                .filter(|ordinal| !blocked(*ordinal))
                .map(|ordinal| u32::try_from(ordinal).unwrap())
                .collect();
            prop_assert_eq!(&delivered, &expected, "each unblocked event is delivered exactly once");
            for key in 0..KEYS {
                let queue = first.store.queue(&[key]).await;
                let expected: Vec<(u32, ParkStatus)> = (0..keys.len())
                    .filter(|ordinal| keys[*ordinal] == key && blocked(*ordinal))
                    .map(|ordinal| {
                        let status = if Some(ordinal) == dead_at[usize::from(key)] {
                            ParkStatus::Dead
                        } else {
                            ParkStatus::Retrying
                        };
                        (u32::try_from(ordinal).unwrap(), status)
                    })
                    .collect();
                prop_assert_eq!(queue, expected, "key {}", key);
            }

            let restarted = TestStore {
                inner: first.store.inner.clone(),
                ..TestStore::default()
            };
            drop(first);
            let second = run_phase(restarted, &keys, &fates, outage_until, lanes).await?;
            check_order(&second.sink.batches.lock().unwrap(), &keys)?;
            let sends = second.sink.sends();
            for sent in &sends {
                prop_assert!(!blocked(usize::try_from(sent.ordinal).unwrap()), "blocked event {} was sent after the restart", sent.ordinal);
            }
            let mut delivered = second.sink.accepted();
            delivered.sort_unstable();
            let replayed: Vec<u32> = (0..keys.len())
                .filter(|ordinal| dead_at[usize::from(keys[*ordinal])].is_none())
                .map(|ordinal| u32::try_from(ordinal).unwrap())
                .collect();
            prop_assert_eq!(
                &delivered,
                &replayed,
                "only keys without a parked queue replay to the sink"
            );
            Ok(())
        })?;
    }

    #[test]
    fn o2_a1_a_crash_in_the_middle_of_parking_loses_no_event(
        keys in proptest::collection::vec(0u8..KEYS, 1..EVENTS),
        codes in proptest::collection::vec(0u8..6, EVENTS),
        store_failures in 0u32..6,
        landed in 0usize..3,
        crash_after_ms in 0u64..4000,
        lanes in 1usize..3,
    ) {
        let fates: Vec<Fate> = codes.iter().copied().map(fate).collect();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let outage_until = Instant::now() + Duration::from_secs(5);
            let store = TestStore {
                append_delay: Duration::from_millis(30),
                ..TestStore::default()
            };
            store.fail_appends.store(store_failures, Ordering::SeqCst);
            store.land_on_failure.store(landed, Ordering::SeqCst);
            let first = start_phase(store, &keys, &fates, outage_until, lanes).await;
            tokio::time::sleep(Duration::from_millis(crash_after_ms)).await;
            check_order(&first.sink.batches.lock().unwrap(), &keys)?;
            let before = first.sink.accepted();
            let inner = first.store.inner.clone();
            drop(first);

            let restarted = TestStore {
                inner: inner.clone(),
                ..TestStore::default()
            };
            let second = run_phase(restarted, &keys, &fates, outage_until, lanes).await?;
            check_order(&second.sink.batches.lock().unwrap(), &keys)?;
            let delivered: HashSet<u32> = before
                .into_iter()
                .chain(second.sink.accepted())
                .collect();
            let mut stored = HashSet::new();
            for key in 0..KEYS {
                let queue = second.store.queue(&[key]).await;
                stored.extend(queue.into_iter().map(|(ordinal, _)| ordinal));
            }
            for ordinal in 0..u32::try_from(keys.len()).unwrap() {
                prop_assert!(
                    delivered.contains(&ordinal) || stored.contains(&ordinal),
                    "event {} of key {} was neither delivered nor kept in the store",
                    ordinal,
                    keys[usize::try_from(ordinal).unwrap()]
                );
            }
            Ok(())
        })?;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operated {
    Flaky(u32),
    Poison,
    Hopeless,
}

fn operated(code: u8) -> Operated {
    match code {
        0..=2 => Operated::Flaky(u32::from(code)),
        3 | 4 => Operated::Poison,
        _ => Operated::Hopeless,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Discarding {
    after_batches: usize,
    ordinal: u32,
}

fn operated_script(fates: Vec<Operated>) -> Script {
    Box::new(
        move |ordinal, attempt| match fates[usize::try_from(ordinal).unwrap()] {
            Operated::Flaky(failures) if attempt <= failures => {
                Outcome::Retryable(SinkError("flaky".to_owned()))
            }
            Operated::Poison if attempt == 1 => Outcome::Terminal(SinkError("poison".to_owned())),
            Operated::Hopeless => Outcome::Terminal(SinkError("hopeless".to_owned())),
            Operated::Flaky(_) | Operated::Poison => Outcome::Delivered,
        },
    )
}

fn check_operated_order(
    batches: &[Vec<Sent>],
    discards: &[Discarding],
    keys: &[u8],
) -> Result<(), TestCaseError> {
    let mut order = KeyOrder::from_first_events(keys);
    let mut discards = discards.iter().peekable();
    for (index, batch) in batches.iter().enumerate() {
        while let Some(discard) = discards.next_if(|discard| discard.after_batches <= index) {
            order.discarded(discard.ordinal, keys);
        }
        order.batch(batch, keys)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn o2_a1_operator_redrives_and_discards_keep_each_key_in_order(
        keys in proptest::collection::vec(0u8..KEYS, 1..EVENTS),
        codes in proptest::collection::vec(0u8..6, EVENTS),
        discards in proptest::collection::vec(any::<bool>(), EVENTS),
        lanes in 1usize..3,
    ) {
        let fates: Vec<Operated> = codes.iter().copied().map(operated).collect();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let setup = Setup {
                lanes,
                concurrency: 2,
                park_after: Duration::from_secs(1),
                ..Setup::default()
            };
            let sink = ScriptedSink::new(LIMITS, operated_script(fates.clone()));
            let mut harness = Harness::start(setup, sink, TestStore::default()).await;
            for (ordinal, key) in keys.iter().enumerate() {
                harness.send(u32::try_from(ordinal).unwrap(), &[*key], 10).unwrap();
            }
            let operator = harness.operator();
            let mut discarding: Vec<Discarding> = Vec::new();
            let mut discarded: HashSet<u32> = HashSet::new();
            for _ in 0..20 {
                tokio::time::sleep(Duration::from_secs(2)).await;
                for (key, head) in harness.store.dead_heads().await {
                    let ordinal = head.ordinal();
                    let index = usize::try_from(ordinal).unwrap();
                    if fates[index] == Operated::Hopeless || discards[index] {
                        let after_batches = harness.sink.batches.lock().unwrap().len();
                        discarding.push(Discarding { after_batches, ordinal });
                        if operator.discard(key, head).await.is_ok() {
                            discarded.insert(ordinal);
                        } else {
                            discarding.pop();
                        }
                    } else {
                        let _ = operator.redrive(key).await;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(120)).await;

            let accepted = harness.sink.accepted();
            for ordinal in 0..u32::try_from(keys.len()).unwrap() {
                let deliveries = accepted.iter().filter(|sent| **sent == ordinal).count();
                let removed = usize::from(discarded.contains(&ordinal));
                prop_assert_eq!(deliveries + removed, 1, "event {} was neither delivered once nor discarded", ordinal);
            }
            check_operated_order(&harness.sink.batches.lock().unwrap(), &discarding, &keys)?;
            let last = Lsn(100 + u64::try_from(keys.len() - 1).unwrap());
            prop_assert_eq!(harness.settle(keys.len()).await, last);
            prop_assert_eq!(harness.ack.inflight_events(), 0);
            prop_assert!(harness.quiet_for(Duration::from_secs(1)).await, "an operator action settled an event");
            for key in 0..KEYS {
                prop_assert_eq!(harness.store.queue(&[key]).await, vec![], "key {}", key);
            }
            Ok(())
        })?;
    }
}
