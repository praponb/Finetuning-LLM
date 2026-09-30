SELECT CASE WHEN age >= 62 THEN 'high' WHEN age >= 30 THEN 'mid' ELSE 'low' END AS age_bucket, COUNT(credit_limit) AS credit_limit_count
FROM customers
GROUP BY CASE WHEN age >= 62 THEN 'high' WHEN age >= 30 THEN 'mid' ELSE 'low' END
HAVING credit_limit_count < 5;
