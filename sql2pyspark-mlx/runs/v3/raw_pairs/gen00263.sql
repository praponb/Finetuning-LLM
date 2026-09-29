SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values FROM employees GROUP BY is_remote ORDER BY level_values ASC, is_remote ASC LIMIT 20;
