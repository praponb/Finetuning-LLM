WITH with_gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    SIZE(COLLECT_SET(country)) AS num_countries,
    MIN(session_id) AS min_session_id
FROM sessionized
GROUP BY GROUPING SETS ((CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', started_at)), (CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END), ())
