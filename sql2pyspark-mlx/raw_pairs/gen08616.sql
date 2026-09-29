WITH filtered AS (
    SELECT *, LOWER(browser) AS browser_lower
    FROM sessions
    WHERE country = 'JP'
),
gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_sessions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    CASE WHEN session_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_tier,
    COUNT(CASE WHEN browser = 'Safari' THEN 1 END) AS safari_count,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    COUNT_IF(device = 'desktop') AS desktop_events
FROM sessions_sessions
GROUP BY 1
HAVING safari_count >= 1
ORDER BY session_id_tier DESC;
