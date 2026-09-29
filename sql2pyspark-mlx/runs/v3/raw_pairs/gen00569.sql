SELECT job_title, SUM(salary) AS total_salary
FROM employees
WHERE job_title IN ('Designer', 'Director', 'Manager')
GROUP BY job_title
HAVING total_salary >= 494000
ORDER BY total_salary DESC;
