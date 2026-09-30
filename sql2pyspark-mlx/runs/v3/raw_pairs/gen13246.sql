SELECT CASE WHEN budget >= 3809000 THEN 'high' WHEN budget >= 1394000 THEN 'mid' ELSE 'low' END budget_bucket, COUNT(DISTINCT budget) unique_locations
FROM departments
WHERE location IN ('New York', 'London', 'Chiang Mai') AND (budget BETWEEN 1731000 AND 2400000 OR dept_name IN ('HR', 'Engineering'))
GROUP BY CASE WHEN budget >= 3809000 THEN 'high' WHEN budget >= 1394000 THEN 'mid' ELSE 'low' END
ORDER BY budget_bucket
LIMIT 3;
