SELECT location, UPPER(location) AS location_upper, CASE WHEN dept_name IN ('Engineering', 'Legal') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group
FROM departments
WHERE budget <= 2046000
ORDER BY dept_name_group;
