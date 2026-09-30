SELECT event_type, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
FROM web_events
WHERE duration_sec >= 173 AND page = '/home'
GROUP BY event_type
HAVING MAX(duration_sec) >= 360
