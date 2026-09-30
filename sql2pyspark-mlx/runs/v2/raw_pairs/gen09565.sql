SELECT
    id,
    location,
    dept_name,
    CAST(budget AS INT) AS budget_int,
    CONCAT(location, ' ', dept_name) AS location_dept_name
FROM departments
WHERE dept_name NOT IN ('Marketing', 'HR') AND budget > 2413000
ORDER BY 1 DESC;
