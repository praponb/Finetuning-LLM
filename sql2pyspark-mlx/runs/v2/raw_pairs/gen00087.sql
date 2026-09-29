SELECT CASE WHEN credit_limit >= 12800 THEN 'high' WHEN credit_limit >= 3400 THEN 'mid' ELSE 'low' END credit_limit_bucket, MAX(age) - MIN(age) age_range
FROM customers
GROUP BY CASE WHEN credit_limit >= 12800 THEN 'high' WHEN credit_limit >= 3400 THEN 'mid' ELSE 'low' END
HAVING MAX(age) - MIN(age) < 51;
