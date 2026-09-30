SELECT CASE WHEN credit_limit >= 12500 THEN 'high' WHEN credit_limit >= 7900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, MIN(credit_limit) AS min_credit_limit, SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count
FROM customers
GROUP BY CASE WHEN credit_limit >= 12500 THEN 'high' WHEN credit_limit >= 7900 THEN 'mid' ELSE 'low' END
HAVING COUNT(DISTINCT credit_limit) >= 1;
