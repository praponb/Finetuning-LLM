SELECT location, total_budget, avg_budget FROM (SELECT location, SUM(budget) AS total_budget, AVG(budget) AS avg_budget FROM departments GROUP BY location) AS s WHERE avg_budget > 2343000;
