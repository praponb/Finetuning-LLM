SELECT is_remote, MIN(hire_date) AS first_hire_date
FROM employees
WHERE (email IS NOT NULL AND job_title IN ('Analyst', 'Intern', 'Director')) OR is_remote
GROUP BY is_remote
ORDER BY first_hire_date
