SELECT
    CASE WHEN credit_limit >= 15900 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS credit_limit_per_london
FROM customers
GROUP BY CASE WHEN credit_limit >= 15900 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY credit_limit_per_london DESC;
