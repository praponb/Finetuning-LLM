SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values FROM employees WHERE dept_id = 26 GROUP BY is_remote;
