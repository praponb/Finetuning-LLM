SELECT
    id,
    budget,
    dept_name,
    LENGTH(location) AS location_length,
    CASE WHEN budget >= 3798000 THEN 'High' WHEN budget >= 1729000 THEN 'Medium' ELSE 'Low' END AS budget_level
FROM departments
WHERE location IS NULL AND dept_name = 'Sales';
