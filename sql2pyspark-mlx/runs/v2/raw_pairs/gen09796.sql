SELECT level, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, MIN(salary) AS min_salary FROM employees WHERE is_remote = TRUE GROUP BY level ORDER BY min_salary DESC;
