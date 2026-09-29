SELECT session_id, SORT_ARRAY(COLLECT_SET(page)) AS page_list
FROM web_events
WHERE TO_DATE(event_time) < '2025-04-01' AND page != '/cart'
GROUP BY session_id
ORDER BY session_id DESC
