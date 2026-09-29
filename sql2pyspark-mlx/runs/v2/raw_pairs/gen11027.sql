SELECT id, manager_id, hire_date, dept_id, FIRST_VALUE(dept_id) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS first_dept_id FROM employees
