SELECT location, SUM(budget) AS total_budget, COUNT(*) AS cnt, ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total FROM departments GROUP BY location;
