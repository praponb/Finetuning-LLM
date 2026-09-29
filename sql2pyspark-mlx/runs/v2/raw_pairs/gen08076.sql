SELECT location, MAX(budget) - MIN(budget) AS budget_range FROM departments GROUP BY location ORDER BY 2;
