SELECT
    id,
    dept_name,
    location,
    SUBSTRING(dept_name, 1, 2) AS dept_name_prefix
FROM departments
WHERE dept_name IN ('Sales', 'HR') AND dept_name = 'Engineering';
