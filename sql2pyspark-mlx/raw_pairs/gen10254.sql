SELECT dept_id, hire_date, DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date
FROM employees
WHERE dept_id = 1
ORDER BY dept_id ASC;
