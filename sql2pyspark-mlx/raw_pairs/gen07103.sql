SELECT id, dept_id, COUNT(*) OVER (PARTITION BY dept_id) AS dept_id_count FROM employees ORDER BY dept_id_count, id DESC;
