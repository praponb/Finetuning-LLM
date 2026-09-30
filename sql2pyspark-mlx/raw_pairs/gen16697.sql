SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values FROM employees WHERE salary BETWEEN 86000 AND 103000 GROUP BY is_remote HAVING MIN(salary) > 58000
