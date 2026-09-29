WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    amount,
    txn_time,
    island_id
FROM sessions_transactions;
