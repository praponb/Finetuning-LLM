SELECT job_title, MAX(salary) - MIN(salary) AS salary_range FROM employees WHERE LENGTH(email) > 11 GROUP BY 1 HAVING MAX(salary) - MIN(salary) > 34;
