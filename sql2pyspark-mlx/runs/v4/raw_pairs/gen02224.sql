SELECT
    city,
    CASE WHEN sqft >= 14092 THEN 'high' WHEN sqft >= 7807 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    MAX(sqft) - MIN(sqft) AS sqft_range,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
GROUP BY city, CASE WHEN sqft >= 14092 THEN 'high' WHEN sqft >= 7807 THEN 'mid' ELSE 'low' END
ORDER BY stddev_sqft
