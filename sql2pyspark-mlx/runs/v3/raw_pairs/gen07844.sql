SELECT id, full_name
FROM employees
WHERE email LIKE '%@outlook.com'
INTERSECT
SELECT id, full_name
FROM employees
WHERE hire_date > '2023-06-26'
