SELECT id, dept_id, CAST(is_remote AS INT) AS is_remote_flag FROM employees ORDER BY dept_id, 1 LIMIT 10;
