WITH flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessionized AS (
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    account_id,
    session_no,
    CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_no_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE merchant = 'Lazada') AS lazada_rows,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    SIZE(COLLECT_SET(merchant)) AS num_merchants_2,
    ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END), 0), 2) AS session_no_per_starbucks,
    MIN(session_no) AS min_session_no
FROM sessionized
GROUP BY ROLLUP (account_id, session_no, CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END)
