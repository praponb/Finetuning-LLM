SELECT id, dept_id, hire_date, salary, ROUND(AVG(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS salary_moving_avg_3 FROM employees
