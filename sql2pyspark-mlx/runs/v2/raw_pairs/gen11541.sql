SELECT
    s.id,
    s.device,
    COUNT(we.id) AS web_event_count,
    SORT_ARRAY(COLLECT_SET(we.event_type)) AS event_type_list,
    MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
GROUP BY s.id, s.device;
