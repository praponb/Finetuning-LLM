SELECT
    id,
    budget,
    dept_name,
    CONCAT(dept_name, '/', location) AS dept_name_location
FROM departments
WHERE location = 'Tokyo' AND (budget > 3762000 OR location IN ('Tokyo', 'Singapore'));
