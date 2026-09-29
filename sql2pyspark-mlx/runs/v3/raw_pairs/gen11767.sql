SELECT
    location,
    MAX(budget) AS max_budget,
    SUM(CASE WHEN location = 'Chiang Mai' THEN budget ELSE 0 END) AS chiang_mai_budget,
    MIN(budget) AS min_budget
FROM departments
GROUP BY location;
