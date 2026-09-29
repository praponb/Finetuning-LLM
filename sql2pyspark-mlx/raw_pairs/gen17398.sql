SELECT
    is_active,
    SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END) AS sg_count,
    SUM(age) AS total_age,
    SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count
FROM customers
WHERE country IN ('UK', 'US')
GROUP BY is_active
ORDER BY sg_count DESC
