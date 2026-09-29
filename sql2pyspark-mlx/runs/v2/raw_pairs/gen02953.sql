SELECT job_title, MAX(salary) AS max_salary
FROM employees
GROUP BY job_title
HAVING MAX(salary) > 138000
