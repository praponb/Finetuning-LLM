SELECT job_title, COUNT(DISTINCT salary) AS unique_managers FROM employees GROUP BY job_title HAVING MAX(salary) < 84000 ORDER BY job_title
