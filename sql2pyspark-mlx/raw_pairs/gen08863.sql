SELECT *, CONCAT_WS(',', dept_name, location) AS dept_name_location_key, FLOOR(budget) AS budget_whole
FROM departments
WHERE dept_name IN ('Marketing', 'Engineering', 'Sales');
