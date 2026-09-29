SELECT
    CASE WHEN credit_limit >= 12700 THEN 'high' WHEN credit_limit >= 7800 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    QUARTER(signup_date) AS signup_quarter,
    SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END) AS london_count,
    COUNT(DISTINCT credit_limit) AS unique_countrys
FROM customers
WHERE credit_limit BETWEEN 8600 AND 15600 AND city IN ('New York', 'Bangkok') AND email IS NOT NULL
GROUP BY CASE WHEN credit_limit >= 12700 THEN 'high' WHEN credit_limit >= 7800 THEN 'mid' ELSE 'low' END, QUARTER(signup_date)
HAVING SUM(CASE WHEN country = 'US' THEN credit_limit ELSE 0 END) < 45
ORDER BY signup_quarter, unique_countrys;
