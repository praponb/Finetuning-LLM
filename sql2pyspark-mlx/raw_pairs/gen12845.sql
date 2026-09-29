SELECT manager_id, MIN(hire_date) AS first_hire_date, MAX(salary) - MIN(salary) AS salary_range
FROM employees
GROUP BY manager_id
