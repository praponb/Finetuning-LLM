WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    is_new_session,
    burst_id,
    CASE WHEN merchant RLIKE '^(7-Eleven|Lazada)$' THEN 'GROUP_B' WHEN merchant RLIKE '^(Grab|Amazon)$' THEN 'CORE' ELSE 'EXTENDED' END AS merchant_zone
FROM sessionized;
