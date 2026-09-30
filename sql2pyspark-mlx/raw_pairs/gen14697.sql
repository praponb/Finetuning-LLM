SELECT location, COUNT(budget) AS budget_count FROM departments WHERE location IN ('Singapore', 'New York', 'Chiang Mai') GROUP BY location HAVING COUNT(budget) > 4;
