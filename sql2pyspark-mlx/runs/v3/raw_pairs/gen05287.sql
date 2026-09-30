SELECT CASE WHEN credit_limit >= 12900 THEN 'high' WHEN credit_limit >= 3800 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, MIN(age) AS min_age FROM customers GROUP BY credit_limit_bucket;
