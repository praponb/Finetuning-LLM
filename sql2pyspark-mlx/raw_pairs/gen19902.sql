WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(merchant) AS merchant_lower
    FROM transactions
    WHERE merchant IS NOT NULL
      AND HOUR(txn_time) BETWEEN 9 AND 12
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY account_id) AS account_id_sum_amount,
        MAX(session_start_flag) OVER (PARTITION BY txn_type, merchant) AS txn_type_max_session_start_flag,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS seq_no,
        SUM(session_start_flag) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS session_start_flag_sum_30d,
        PERCENT_RANK() OVER (ORDER BY island_id) AS island_id_percent_rank
    FROM islands
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN prev_txn_time - LAG(prev_txn_time) OVER (PARTITION BY merchant ORDER BY prev_txn_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
islands_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY prev_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY session_start_flag DESC NULLS LAST, id) AS row_num
    FROM islands_transactions
    QUALIFY row_num = 1
)
SELECT
    id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    merchant_lower,
    prev_txn_time,
    session_start_flag,
    island_id,
    CASE WHEN merchant RLIKE '^(7-Eleven|Grab)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS merchant_zone
FROM deduped;
