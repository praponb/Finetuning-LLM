WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        TRIM(txn_type) AS clean_txn_type
    FROM transactions
    WHERE merchant NOT IN ('Shell', 'Lazada', 'Amazon')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        clean_txn_type,
        SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY TO_DATE(txn_time), merchant ORDER BY amount DESC) AS amount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS event_seq
    FROM sessions_transactions
    WHERE merchant IN ('Grab', 'Starbucks')
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM with_windows
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        session_id,
        event_seq,
        SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        amount,
        merchant,
        session_id,
        event_seq,
        session_id_2,
        event_seq * amount AS event_seq_times_amount
    FROM islands
    WHERE txn_time > DATE '2023-10-11'
      AND merchant IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY event_seq DESC, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY merchant ORDER BY event_seq, id) AS event_seq_bucket,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS event_seq_2,
        NTILE(10) OVER (PARTITION BY merchant ORDER BY event_seq DESC, id) AS event_seq_bucket_2
    FROM ranked
)
SELECT
    id,
    amount,
    event_seq,
    session_id_2,
    event_seq_times_amount,
    event_seq_bucket_2
FROM with_windows_transactions
ORDER BY event_seq_times_amount DESC;
