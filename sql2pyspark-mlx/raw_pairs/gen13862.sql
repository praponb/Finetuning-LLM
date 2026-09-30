SELECT age_bucket, unique_countrys
FROM (
    SELECT CASE WHEN age >= 72 THEN 'high' WHEN age >= 24 THEN 'mid' ELSE 'low' END AS age_bucket, COUNT(DISTINCT age) AS unique_countrys
    FROM customers
    GROUP BY CASE WHEN age >= 72 THEN 'high' WHEN age >= 24 THEN 'mid' ELSE 'low' END
) AS s
WHERE unique_countrys < 8
