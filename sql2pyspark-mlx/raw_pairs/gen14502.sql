SELECT full_name
FROM employees
WHERE LENGTH(email) < 8
UNION
SELECT browser
FROM sessions
ORDER BY full_name;
