SELECT CASE WHEN age >= 58 THEN 'high' WHEN age >= 41 THEN 'mid' ELSE 'low' END AS age_bucket, COUNT(DISTINCT age) AS unique_segments
FROM customers
WHERE age = 49
GROUP BY CASE WHEN age >= 58 THEN 'high' WHEN age >= 41 THEN 'mid' ELSE 'low' END
HAVING COUNT(DISTINCT age) >= 1
ORDER BY unique_segments, age_bucket;
