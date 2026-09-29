SELECT CASE WHEN credit_limit >= 17500 THEN 'high' WHEN credit_limit >= 7500 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, COUNT(age) AS age_count, ROUND((100 * SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_corporate
FROM customers
WHERE city <> 'New York' AND is_active = FALSE
GROUP BY CASE WHEN credit_limit >= 17500 THEN 'high' WHEN credit_limit >= 7500 THEN 'mid' ELSE 'low' END
ORDER BY pct_corporate DESC, credit_limit_bucket
LIMIT 50;
