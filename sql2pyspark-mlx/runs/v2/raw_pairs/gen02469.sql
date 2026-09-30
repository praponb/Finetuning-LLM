SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    city,
    COUNT(DISTINCT sqft) AS unique_regions,
    ROUND((100 * SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york
FROM stores
WHERE sqft <= 10686 OR opened_date <= '2025-05-01'
GROUP BY 1, 2
