SELECT
    budget,
    dept_name,
    location,
    LOWER(dept_name) AS dept_name_lower
FROM departments
WHERE dept_name IN ('Finance', 'Legal', 'HR');
