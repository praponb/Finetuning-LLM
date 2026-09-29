SELECT location, MAX(budget) - MIN(budget) AS budget_range, MAX(budget) AS max_budget, SUM(CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END) AS hr_count FROM departments GROUP BY location;
