SELECT is_remote, YEAR(hire_date) AS hire_year, COUNT(*) AS num_employees
FROM employees
WHERE job_title <> 'Director' AND (manager_id = 33 OR job_title IN ('Intern', 'Designer', 'Director'))
GROUP BY is_remote, YEAR(hire_date)
ORDER BY num_employees DESC
