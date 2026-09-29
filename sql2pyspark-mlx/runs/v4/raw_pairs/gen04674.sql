WITH with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) AS prev_txn_time
    FROM transactions
),
gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    account_id,
    amount,
    txn_time,
    merchant,
    is_flagged,
    prev_txn_time,
    burst_id
FROM islands;
