SELECT id, dept_id, hire_date, salary, LEAD(salary, 1, 0) OVER (PARTITION BY dept_id ORDER BY hire_date, id) next_salary FROM employees ORDER BY hire_date, id DESC
