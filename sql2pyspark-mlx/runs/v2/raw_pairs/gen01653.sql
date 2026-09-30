SELECT location, SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) AS sales_budget, SUM(budget) AS total_budget, MIN(budget) AS min_budget FROM departments GROUP BY location ORDER BY 3 DESC;
