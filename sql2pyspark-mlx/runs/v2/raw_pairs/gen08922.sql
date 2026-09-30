SELECT
    id,
    budget,
    location,
    dept_name,
    CASE WHEN location = 'London' THEN 1 ELSE 0 END AS is_london
FROM departments
WHERE location = 'New York'
