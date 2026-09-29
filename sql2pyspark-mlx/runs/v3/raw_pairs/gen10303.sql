SELECT level
FROM employees
WHERE level = 'L4'
UNION ALL
SELECT level
FROM employees
WHERE LENGTH(email) > 5;
