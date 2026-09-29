SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary, SUM(salary) AS total_salary, MAX(salary) AS max_salary FROM employees GROUP BY dept_id HAVING total_salary < 568000;
