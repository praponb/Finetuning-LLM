SELECT
    DATE_TRUNC('MONTH', event_time) event_month,
    session_id,
    MAX(event_time) last_event_time,
    ROUND((100 * SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_search
FROM web_events
GROUP BY DATE_TRUNC('MONTH', event_time), session_id
HAVING pct_search < 73
ORDER BY last_event_time DESC
