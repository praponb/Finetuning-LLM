SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') opened_month,
    city,
    COUNT(sqft) sqft_count,
    COUNT(*) total_count
FROM stores
WHERE region != 'East'
GROUP BY 1, 2
ORDER BY opened_month, city DESC
LIMIT 10
