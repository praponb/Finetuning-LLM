SELECT
    city,
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    MAX(sqft) - MIN(sqft) AS sqft_range,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
    AVG(sqft) AS avg_sqft
FROM stores
WHERE sqft = 9308 OR store_name LIKE '%Mall%'
GROUP BY city, DATE_TRUNC('MONTH', opened_date)
