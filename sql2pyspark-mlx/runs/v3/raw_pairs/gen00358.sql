SELECT level
FROM employees
WHERE LENGTH(email) > 12
UNION ALL
SELECT level
FROM employees
WHERE salary > 137000
ORDER BY level DESC
