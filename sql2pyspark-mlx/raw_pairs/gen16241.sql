SELECT ROUND(AVG(budget), 2) avg_budget, SUM(CASE WHEN location = 'London' THEN 1 ELSE 0 END) london_count, SUM(CASE WHEN location = 'Berlin' THEN 1 ELSE 0 END) berlin_count
FROM departments
WHERE location IN ('New York', 'Chiang Mai') AND budget >= 3857000
ORDER BY avg_budget DESC, 2 DESC
