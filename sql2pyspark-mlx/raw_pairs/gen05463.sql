WITH gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessions_transactions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    session_start_flag,
    island_id,
    CASE WHEN txn_type RLIKE '^(REFUND|CREDIT)$' THEN 'GROUP_B' WHEN txn_type RLIKE '^(DEBIT|FEE)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS txn_type_zone
FROM sessions_transactions
