SELECT session_id, SORT_ARRAY(COLLECT_SET(page)) AS page_list, MAX(event_time) AS last_event_time
FROM web_events
GROUP BY session_id
ORDER BY last_event_time, session_id
