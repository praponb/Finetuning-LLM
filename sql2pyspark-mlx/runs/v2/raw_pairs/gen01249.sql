SELECT id, location, COALESCE(budget, 0) AS budget_filled
FROM departments
WHERE location = 'Singapore' AND dept_name = 'Legal';
