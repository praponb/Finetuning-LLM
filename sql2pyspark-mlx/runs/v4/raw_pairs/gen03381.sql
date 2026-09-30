SELECT
    id,
    budget,
    dept_name,
    CASE WHEN dept_name IN ('Sales', 'Finance') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group
FROM departments
WHERE location IN ('Singapore', 'Tokyo', 'New York')
ORDER BY dept_name, id
LIMIT 3
