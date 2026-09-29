WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    account_id,
    txn_time,
    merchant,
    island_id,
    CASE WHEN merchant RLIKE '^(Lazada|Starbucks)$' THEN 'CORE' WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS merchant_zone
FROM sessionized
ORDER BY island_id DESC NULLS LAST;
