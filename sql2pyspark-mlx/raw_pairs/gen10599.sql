SELECT CASE WHEN age >= 59 THEN 'high' WHEN age >= 37 THEN 'mid' ELSE 'low' END AS age_bucket, MIN(age) AS min_age, COUNT(DISTINCT credit_limit) AS unique_citys
FROM customers
WHERE country IN ('DE', 'SG')
GROUP BY CASE WHEN age >= 59 THEN 'high' WHEN age >= 37 THEN 'mid' ELSE 'low' END
ORDER BY age_bucket;
