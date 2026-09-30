SELECT location, ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END), 0), 2) AS budget_per_sales, COUNT(*) AS num_rows FROM departments GROUP BY location HAVING COUNT(*) > 1
