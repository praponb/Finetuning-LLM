WITH with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    ROUND(AVG(burst_id), 2) AS avg_burst_id,
    MAX(paid_at) AS ended_at,
    APPROX_PERCENTILE(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles
FROM sessions_payments
GROUP BY quarter_start
ORDER BY avg_burst_id NULLS LAST
