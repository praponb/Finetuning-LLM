SELECT id, dept_id, salary FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rnk FROM employees) AS r WHERE rnk <= 5
