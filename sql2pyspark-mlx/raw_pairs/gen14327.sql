WITH gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessions_transactions AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    is_flagged,
    burst_id,
    CASE WHEN txn_type RLIKE '^(REFUND|FEE)$' THEN 'CORE' ELSE 'GROUP_B' END AS txn_type_zone
FROM sessions_transactions
ORDER BY txn_type DESC NULLS FIRST;
