SELECT id, job_title, hire_date, dept_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS rnk FROM employees) AS r WHERE rnk = 1;
