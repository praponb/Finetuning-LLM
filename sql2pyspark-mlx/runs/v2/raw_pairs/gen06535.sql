SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values, COUNT(*) AS num_web_events
FROM web_events
ORDER BY 2 ASC
