SELECT location, COUNT(budget) AS budget_count FROM departments WHERE location IN ('Singapore', 'Berlin', 'Bangkok') GROUP BY location HAVING budget_count > 1 ORDER BY location
