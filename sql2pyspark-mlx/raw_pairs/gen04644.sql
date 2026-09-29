SELECT *
FROM (
    SELECT CASE WHEN credit_limit >= 16700 THEN 'high' WHEN credit_limit >= 8300 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, COUNT(*) AS cnt
    FROM customers
    GROUP BY CASE WHEN credit_limit >= 16700 THEN 'high' WHEN credit_limit >= 8300 THEN 'mid' ELSE 'low' END
) AS s
WHERE cnt >= 7;
