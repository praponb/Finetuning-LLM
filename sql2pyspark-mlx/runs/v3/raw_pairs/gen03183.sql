SELECT dept_name, SUM(budget) total_budget, ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total
FROM departments
GROUP BY dept_name
