SELECT
    QUARTER(signup_date) AS signup_quarter,
    COUNT(*) AS num_customers,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM customers
GROUP BY QUARTER(signup_date)
HAVING COUNT(CASE WHEN country = 'DE' THEN 1 END) > 4
ORDER BY de_count DESC, chiang_mai_count DESC
