SELECT
    id,
    budget,
    dept_name,
    location,
    LOWER(dept_name) AS dept_name_lower
FROM departments
WHERE budget <= 2424000
ORDER BY budget DESC
