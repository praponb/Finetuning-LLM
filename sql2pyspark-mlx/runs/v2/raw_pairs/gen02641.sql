SELECT location, MAX(budget) - MIN(budget) AS budget_range, SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) AS engineering_count FROM departments GROUP BY location
