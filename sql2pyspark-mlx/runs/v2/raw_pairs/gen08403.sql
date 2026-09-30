SELECT page, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) page_values, MIN(duration_sec) min_duration_sec FROM web_events GROUP BY page ORDER BY min_duration_sec DESC;
