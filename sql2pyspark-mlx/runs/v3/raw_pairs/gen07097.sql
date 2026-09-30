SELECT
    id,
    dept_name,
    location,
    budget,
    COALESCE(dept_name, 'unassigned') dept_name_filled
FROM departments;
