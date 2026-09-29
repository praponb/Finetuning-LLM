SELECT * FROM (SELECT CASE WHEN credit_limit >= 17200 THEN 'high' WHEN credit_limit >= 2800 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, city, age FROM customers WHERE country IN ('US', 'TH', 'UK'))
PIVOT (COUNT(age) FOR city IN ('Berlin', 'Singapore', 'New York', 'Tokyo'))
