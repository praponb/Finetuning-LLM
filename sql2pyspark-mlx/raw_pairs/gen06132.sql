WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    ROUND(AVG(new_group_flag), 2) AS avg_new_group_flag,
    ROUND(SUM(new_group_flag) / NULLIF(COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END), 0), 2) AS new_group_flag_per_refund,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count
FROM islands
GROUP BY CUBE (DATE_TRUNC('MONTH', txn_time));
