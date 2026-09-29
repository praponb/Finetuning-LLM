SELECT location, SUM(CASE WHEN location = 'Chiang Mai' THEN budget ELSE 0 END) AS chiang_mai_budget
FROM departments
WHERE location = 'Berlin'
GROUP BY location
HAVING COUNT(DISTINCT budget) > 4
