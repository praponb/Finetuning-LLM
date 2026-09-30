SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list, SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END) AS search_count FROM web_events GROUP BY 1;
