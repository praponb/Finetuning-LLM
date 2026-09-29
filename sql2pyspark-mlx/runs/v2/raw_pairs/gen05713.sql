SELECT job_title, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY job_title
HAVING avg_salary > 38;
