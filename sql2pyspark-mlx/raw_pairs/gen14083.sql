WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Grouped metrics
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN merchant = 'Shell' THEN 1 END), 0), 2) AS is_new_burst_per_shell,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN session_id ELSE 0 END) AS credit_session_id,
    PERCENTILE_APPROX(is_new_burst, ARRAY(0.25, 0.5, 0.75)) AS is_new_burst_quartiles,
    MIN(txn_time) AS first_seen
FROM sessionized
GROUP BY CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END
