SELECT COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count, COUNT(DISTINCT budget) AS unique_locations, SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) AS bangkok_budget
FROM departments
ORDER BY bangkok_budget DESC, unique_locations DESC;
