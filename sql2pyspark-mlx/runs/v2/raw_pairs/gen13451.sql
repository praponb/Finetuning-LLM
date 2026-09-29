SELECT page, event_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
FROM web_events
WHERE session_id IN (8, 15) AND session_id IN (16, 31)
GROUP BY page, event_type;
