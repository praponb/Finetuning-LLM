SELECT is_remote, MIN(salary) min_salary, SUM(CASE WHEN job_title = 'Director' THEN salary ELSE 0 END) director_salary
FROM employees
WHERE MONTH(hire_date) IN (1, 4, 6) AND LENGTH(email) >= 9
GROUP BY is_remote
