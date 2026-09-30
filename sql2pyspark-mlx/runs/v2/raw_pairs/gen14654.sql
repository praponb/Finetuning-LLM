SELECT
    id,
    budget,
    location,
    dept_name,
    LOWER(dept_name) dept_name_lower
FROM departments
WHERE budget > 3419000;
