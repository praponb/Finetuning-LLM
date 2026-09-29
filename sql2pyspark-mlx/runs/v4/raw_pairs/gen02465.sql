SELECT id, location, dept_name, budget, ROUND(budget * 1.1, 2) AS adjusted_budget, CONCAT_WS(',', location, dept_name) AS location_dept_name_key FROM departments ORDER BY location_dept_name_key
