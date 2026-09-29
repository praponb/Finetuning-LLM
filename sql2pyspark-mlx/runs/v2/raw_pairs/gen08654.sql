SELECT is_remote, MAX(hire_date) latest_hire_date, MIN(hire_date) min_hire_date
FROM employees
GROUP BY is_remote
ORDER BY min_hire_date;
