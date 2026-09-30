SELECT * FROM (SELECT location, SUM(budget) AS total_budget FROM departments GROUP BY location) AS s WHERE total_budget >= 12939000
