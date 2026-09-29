SELECT
    s.id,
    s.country,
    COUNT(we.id) AS web_event_count,
    SORT_ARRAY(COLLECT_SET(we.page)) AS page_list,
    COUNT(DISTINCT we.duration_sec) AS unique_sessions
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
GROUP BY s.id, s.country
ORDER BY s.id ASC
