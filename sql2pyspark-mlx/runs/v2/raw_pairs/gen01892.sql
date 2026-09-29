SELECT hire_date, full_name
FROM employees
WHERE email LIKE '%@gmail.com'
UNION
SELECT hire_date, full_name
FROM employees
WHERE dept_id = 40
ORDER BY hire_date
