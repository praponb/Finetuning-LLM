SELECT age_bucket, total_age, unique_citys
FROM (
    SELECT CASE WHEN age >= 59 THEN 'high' WHEN age >= 26 THEN 'mid' ELSE 'low' END AS age_bucket, SUM(age) AS total_age, COUNT(DISTINCT age) AS unique_citys
    FROM customers
    GROUP BY CASE WHEN age >= 59 THEN 'high' WHEN age >= 26 THEN 'mid' ELSE 'low' END
) AS t
WHERE unique_citys >= 7
ORDER BY total_age ASC, age_bucket ASC
LIMIT 5
