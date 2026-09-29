SELECT segment, credit_limit_bucket, stddev_age
FROM (
    SELECT segment, CASE WHEN credit_limit >= 15300 THEN 'high' WHEN credit_limit >= 5900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, ROUND(STDDEV(age), 2) AS stddev_age
    FROM customers
    GROUP BY segment, CASE WHEN credit_limit >= 15300 THEN 'high' WHEN credit_limit >= 5900 THEN 'mid' ELSE 'low' END
) AS agg
WHERE stddev_age > 53;
