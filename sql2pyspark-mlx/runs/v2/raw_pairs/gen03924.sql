SELECT
    CASE WHEN age >= 55 THEN 'high' WHEN age >= 43 THEN 'mid' ELSE 'low' END AS age_bucket,
    city,
    SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END) AS new_york_count,
    COUNT(age) AS age_count,
    SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
FROM customers
WHERE age IS NOT NULL
GROUP BY CASE WHEN age >= 55 THEN 'high' WHEN age >= 43 THEN 'mid' ELSE 'low' END, city
ORDER BY bangkok_count, age_bucket, city
LIMIT 10
