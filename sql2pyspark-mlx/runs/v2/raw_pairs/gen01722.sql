SELECT * FROM (SELECT CASE WHEN credit_limit >= 12500 THEN 'high' WHEN credit_limit >= 4400 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, country, credit_limit FROM customers)
PIVOT (COUNT(credit_limit) FOR country IN ('DE', 'TH', 'UK', 'US'));
