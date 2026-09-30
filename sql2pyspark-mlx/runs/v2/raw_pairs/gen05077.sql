SELECT
    location,
    budget,
    dept_name,
    SUBSTRING(dept_name, 1, 3) AS dept_name_prefix
FROM departments
