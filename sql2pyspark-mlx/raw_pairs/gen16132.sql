SELECT
    city,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count,
    COUNT(*) AS row_count
FROM stores
GROUP BY 1;
