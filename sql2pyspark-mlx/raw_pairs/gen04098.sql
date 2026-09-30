WITH flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM flagged
