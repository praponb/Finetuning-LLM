SELECT id, dept_name, CASE WHEN budget >= 3236000 THEN 'High' WHEN budget >= 1220000 THEN 'Medium' ELSE 'Low' END AS budget_level FROM departments ORDER BY dept_name, budget_level DESC, id LIMIT 10;
