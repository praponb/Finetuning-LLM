SELECT CASE WHEN age >= 58 THEN 'high' WHEN age >= 27 THEN 'mid' ELSE 'low' END AS age_bucket, SUM(credit_limit) AS total_credit_limit
FROM customers
GROUP BY CASE WHEN age >= 58 THEN 'high' WHEN age >= 27 THEN 'mid' ELSE 'low' END
ORDER BY total_credit_limit DESC;
