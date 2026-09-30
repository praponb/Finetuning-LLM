SELECT
    QUARTER(event_time) AS event_quarter,
    session_id,
    SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
FROM web_events
WHERE event_type = 'purchase' OR event_type IN ('click', 'search', 'purchase')
GROUP BY event_quarter, session_id
