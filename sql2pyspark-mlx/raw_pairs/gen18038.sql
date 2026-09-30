SELECT id, email, dept_id, manager_id, salary, hire_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS latest_rank FROM employees) x WHERE latest_rank = 1
