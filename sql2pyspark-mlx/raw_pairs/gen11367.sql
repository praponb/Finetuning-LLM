SELECT
    segment,
    is_active,
    ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london,
    SUM(CASE WHEN segment = 'Corporate' THEN age ELSE 0 END) AS corporate_age,
    COUNT(credit_limit) AS credit_limit_count
FROM customers
GROUP BY 1, 2
ORDER BY corporate_age DESC
