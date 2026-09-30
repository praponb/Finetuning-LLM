SELECT
    s.id,
    s.browser,
    COUNT(we.id) AS web_event_count,
    ROUND(AVG(we.duration_sec), 2) AS avg_duration_sec,
    ROUND((100 * SUM(CASE WHEN we.page = '/search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search
FROM sessions s
LEFT OUTER JOIN web_events we ON s.id = we.session_id
GROUP BY s.id, s.browser;
