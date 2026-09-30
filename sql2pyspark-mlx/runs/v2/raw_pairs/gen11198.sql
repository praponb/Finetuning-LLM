SELECT
    id,
    location,
    budget,
    SUBSTRING(dept_name, 1, 4) AS dept_name_prefix
FROM departments
WHERE budget <= 2847000
