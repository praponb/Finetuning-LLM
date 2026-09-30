SELECT *, CASE WHEN budget >= 3832000 THEN 'High' WHEN budget >= 1885000 THEN 'Medium' ELSE 'Low' END AS budget_level
FROM departments
WHERE location IN ('Berlin', 'Chiang Mai')
ORDER BY location, id DESC
