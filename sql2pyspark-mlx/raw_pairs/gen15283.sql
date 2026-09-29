SELECT id, location
FROM departments
WHERE location IS NULL
ORDER BY location DESC
