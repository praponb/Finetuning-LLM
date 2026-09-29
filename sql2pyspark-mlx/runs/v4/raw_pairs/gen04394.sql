SELECT location, SUM(budget) total_budget, COUNT(*) cnt, ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total FROM departments GROUP BY location
