WITH base AS (
    SELECT *, ROUND(duration_sec * 100, 2) AS duration_sec_adjusted
    FROM web_events
    WHERE duration_sec IS NOT NULL OR event_type = 'purchase'
),
summary AS (
    SELECT session_id, ROUND((100 * SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home, MAX(event_time) AS latest_event_time
    FROM base
    GROUP BY session_id
)
SELECT *
FROM summary
WHERE pct_home > 58
ORDER BY session_id DESC
LIMIT 3
