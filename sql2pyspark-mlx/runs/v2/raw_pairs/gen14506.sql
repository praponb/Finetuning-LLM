SELECT
    id,
    dept_name,
    budget,
    location,
    LOWER(dept_name) AS dept_name_lower
FROM departments
WHERE budget <= 1754000;
