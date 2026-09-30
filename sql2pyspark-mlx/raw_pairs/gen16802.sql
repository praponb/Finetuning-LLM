WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
)
-- Running sum of the flag = session number
SELECT
    id,
    account_id,
    amount,
    txn_time,
    merchant,
    SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM gaps;
