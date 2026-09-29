SELECT id, budget, CONCAT_WS('-', location, dept_name) AS location_dept_name_key
FROM departments
WHERE budget <= 2823000
