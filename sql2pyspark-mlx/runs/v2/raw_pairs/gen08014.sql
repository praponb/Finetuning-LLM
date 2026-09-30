SELECT
    id,
    location,
    budget,
    dept_name,
    LENGTH(location) AS location_length,
    CASE WHEN budget >= 3053000 THEN 'High' WHEN budget >= 1016000 THEN 'Medium' ELSE 'Low' END AS budget_band
FROM departments
WHERE budget >= 2252000 AND dept_name = 'Sales'
ORDER BY location_length DESC;
