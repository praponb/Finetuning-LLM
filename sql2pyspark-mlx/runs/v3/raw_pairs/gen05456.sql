SELECT HOUR(event_time) event_hour, session_id, page, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) page_values FROM web_events GROUP BY HOUR(event_time), session_id, page ORDER BY page_values;
