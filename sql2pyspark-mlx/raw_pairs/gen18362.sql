WITH with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_sessions AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('WEEK', prev_started_at) AS week_start,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN browser = 'Edge' THEN 1 END), 0), 2) AS session_id_per_edge,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(DISTINCT browser) AS distinct_browsers
FROM sessions_sessions
GROUP BY DATE_TRUNC('WEEK', prev_started_at);
