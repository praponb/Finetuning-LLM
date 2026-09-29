SELECT
    id,
    location,
    dept_name,
    budget,
    TRIM(location) AS clean_location
FROM departments
WHERE location = 'Tokyo' AND budget < 1429000 AND dept_name != 'Engineering'
ORDER BY id DESC
