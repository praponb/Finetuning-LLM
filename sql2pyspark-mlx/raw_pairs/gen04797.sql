SELECT is_remote, MAX(hire_date) AS latest_hire_date
FROM employees
WHERE job_title = 'Manager'
GROUP BY is_remote;
