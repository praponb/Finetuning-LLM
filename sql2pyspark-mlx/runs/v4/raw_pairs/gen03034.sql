SELECT QUARTER(event_time) AS event_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values FROM web_events GROUP BY event_quarter ORDER BY page_values, event_quarter LIMIT 10;
