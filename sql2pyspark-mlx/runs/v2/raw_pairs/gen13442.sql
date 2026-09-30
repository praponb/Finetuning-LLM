SELECT id, hire_date, salary, SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary FROM employees ORDER BY running_salary DESC;
