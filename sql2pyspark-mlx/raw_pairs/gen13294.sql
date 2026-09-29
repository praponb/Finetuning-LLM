WITH with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
)
SELECT
    id,
    account_id,
    amount,
    txn_time,
    merchant,
    is_flagged,
    SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM with_gaps
