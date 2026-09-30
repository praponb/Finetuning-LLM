SELECT CASE WHEN age >= 62 THEN 'high' WHEN age >= 33 THEN 'mid' ELSE 'low' END AS age_bucket, MAX(age) - MIN(age) AS age_range FROM customers GROUP BY age_bucket
