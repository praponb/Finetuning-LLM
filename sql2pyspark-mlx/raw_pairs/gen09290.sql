WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
)
-- Running sum of the flag = session number
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM flagged
