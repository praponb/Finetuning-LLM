SELECT job_title, AVG(salary) AS avg_salary FROM employees GROUP BY job_title HAVING avg_salary > 84000 ORDER BY avg_salary DESC
