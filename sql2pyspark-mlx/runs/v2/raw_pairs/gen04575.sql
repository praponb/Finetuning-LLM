SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list FROM web_events WHERE session_id IN (18, 25) GROUP BY session_id HAVING COUNT(*) > 1 ORDER BY session_id DESC;
