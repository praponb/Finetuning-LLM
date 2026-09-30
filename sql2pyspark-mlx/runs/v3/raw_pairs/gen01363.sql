SELECT dept_name, CASE WHEN budget >= 3189000 THEN 'High' WHEN budget >= 1290000 THEN 'Medium' ELSE 'Low' END AS budget_band
FROM departments
WHERE location IN ('Chiang Mai', 'London', 'Singapore') AND dept_name = 'Finance'
ORDER BY budget_band
