SELECT COUNT(duration_sec) AS duration_sec_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values FROM web_events ORDER BY 1
