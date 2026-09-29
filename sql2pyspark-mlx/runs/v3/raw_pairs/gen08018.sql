SELECT
    id,
    dept_name,
    budget,
    location,
    CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END AS is_hr
FROM departments
