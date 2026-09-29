WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.is_new_session,
    CASE WHEN txn_type RLIKE '^(FEE|REFUND)$' THEN 'EXTENDED' WHEN txn_type RLIKE '^(DEBIT|CREDIT)$' THEN 'CORE' ELSE 'PRIORITY' END AS txn_type_zone,
    (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS min_transaction_amount
FROM sessionized s
ORDER BY txn_type, txn_type_zone
