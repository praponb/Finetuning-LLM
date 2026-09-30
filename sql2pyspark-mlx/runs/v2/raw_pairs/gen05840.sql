SELECT level, job_title, MIN(salary) AS min_salary
FROM employees
GROUP BY level, job_title
ORDER BY job_title DESC, level
LIMIT 50;
