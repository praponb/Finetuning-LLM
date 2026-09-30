SELECT full_name, hire_date, level
FROM employees
WHERE dept_id IN (5, 8, 21, 28)
ORDER BY level ASC;
