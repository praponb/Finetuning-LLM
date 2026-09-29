SELECT location, AVG(budget) AS avg_budget FROM departments WHERE location NOT IN ('Bangkok', 'Chiang Mai') GROUP BY location
