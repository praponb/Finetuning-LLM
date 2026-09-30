WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    txn_time,
    session_id
FROM sessionized
ORDER BY id DESC;
