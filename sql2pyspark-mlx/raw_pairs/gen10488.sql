SELECT full_name
FROM employees
WHERE manager_id = 21
UNION ALL
SELECT full_name
FROM employees
WHERE email LIKE '%@outlook.com';
