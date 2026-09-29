SELECT
    region,
    SUM(CASE WHEN region = 'North' THEN 1 ELSE 0 END) north_count,
    SUM(sqft) total_sqft,
    COUNT(sqft) sqft_count
FROM stores
WHERE region = 'North' AND store_name LIKE 'A%'
GROUP BY region;
