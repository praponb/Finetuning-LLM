SELECT id, budget, CASE WHEN budget >= 3935000 THEN 'High' WHEN budget >= 1276000 THEN 'Medium' ELSE 'Low' END AS budget_band
FROM departments
WHERE location NOT IN ('Chiang Mai', 'London', 'Berlin') AND budget < 3846000
