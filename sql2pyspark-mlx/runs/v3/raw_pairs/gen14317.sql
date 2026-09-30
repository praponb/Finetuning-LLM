SELECT
    id,
    location,
    budget,
    dept_name
FROM departments
WHERE dept_name = 'Legal' AND budget BETWEEN 1218000 AND 2279000;
