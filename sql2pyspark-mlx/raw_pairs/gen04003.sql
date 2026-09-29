SELECT id, hire_date, full_name
FROM employees
WHERE hire_date > (SELECT MAX(hire_date) FROM employees)
ORDER BY full_name DESC, hire_date DESC;
