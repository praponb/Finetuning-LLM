SELECT
    region,
    SUM(sqft) AS total_sqft,
    COUNT(*) AS cnt,
    SUM(SUM(sqft)) OVER (ORDER BY region) AS cumulative_sqft
FROM stores
WHERE store_name LIKE '%Kiosk%'
GROUP BY region
