WITH prev_events AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
)
SELECT
    id,
    txn_type,
    txn_time,
    merchant,
    SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM gaps;
