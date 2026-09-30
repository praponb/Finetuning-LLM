SELECT MAX(hire_date) AS latest_hire_date, AVG(salary) AS avg_salary, ROUND(STDDEV(salary), 2) AS stddev_salary FROM employees WHERE dept_id = 22 AND job_title <> 'Analyst' ORDER BY avg_salary;
