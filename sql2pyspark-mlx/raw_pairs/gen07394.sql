WITH gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
)
SELECT
    id,
    txn_type,
    txn_time,
    SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM gaps
