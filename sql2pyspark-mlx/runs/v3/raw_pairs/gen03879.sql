SELECT
    budget,
    dept_name,
    location,
    CASE WHEN dept_name IN ('Sales', 'Engineering') THEN 'Priority' ELSE 'Other' END AS dept_name_group
FROM departments
ORDER BY dept_name_group DESC;
