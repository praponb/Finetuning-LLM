SELECT *
FROM departments
WHERE location = 'Berlin' OR budget >= 2475000
ORDER BY 3, id
LIMIT 10;
