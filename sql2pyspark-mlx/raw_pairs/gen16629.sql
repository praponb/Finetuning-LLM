SELECT job_title, AVG(salary) AS avg_salary FROM employees WHERE hire_date <= '2024-07-01' GROUP BY job_title HAVING AVG(salary) >= 110000;
