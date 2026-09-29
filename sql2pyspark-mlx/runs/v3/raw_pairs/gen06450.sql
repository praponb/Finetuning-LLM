SELECT job_title, MAX(salary) - MIN(salary) AS salary_range
FROM employees
GROUP BY job_title
HAVING MAX(salary) - MIN(salary) < 59
ORDER BY salary_range ASC;
