WITH session_summary AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, ROUND((100 * SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    ss.stddev_duration_sec,
    ss.pct_search
FROM sessions s
LEFT OUTER JOIN session_summary ss ON s.id = ss.session_id
