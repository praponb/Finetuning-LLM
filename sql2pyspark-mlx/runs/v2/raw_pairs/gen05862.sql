SELECT id, hire_date
FROM employees
WHERE hire_date = (SELECT MIN(hire_date) FROM employees)
ORDER BY id DESC, hire_date DESC
LIMIT 5;
