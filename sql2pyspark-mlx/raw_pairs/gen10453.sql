WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM with_gaps
