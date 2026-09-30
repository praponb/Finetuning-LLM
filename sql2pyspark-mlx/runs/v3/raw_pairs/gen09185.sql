SELECT level, SUM(salary) AS total_salary, ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total FROM employees GROUP BY level ORDER BY level LIMIT 10;
