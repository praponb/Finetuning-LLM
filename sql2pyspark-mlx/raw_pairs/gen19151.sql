SELECT HOUR(event_time) AS event_hour, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list FROM web_events WHERE event_type = 'add_to_cart' GROUP BY 1 ORDER BY event_hour DESC LIMIT 5;
