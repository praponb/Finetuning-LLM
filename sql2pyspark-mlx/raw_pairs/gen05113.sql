SELECT id, location, budget, dept_name, CASE WHEN dept_name IN ('Finance', 'Marketing') THEN 'Priority' ELSE 'Other' END AS dept_name_group FROM departments ORDER BY dept_name_group DESC;
