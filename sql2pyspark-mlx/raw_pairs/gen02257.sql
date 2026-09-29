SELECT
    CASE WHEN sqft >= 15479 THEN 'high' WHEN sqft >= 6028 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    city,
    MIN(sqft) AS min_sqft,
    COUNT(sqft) AS sqft_count
FROM stores
GROUP BY 1, 2
ORDER BY city ASC, sqft_bucket ASC
LIMIT 50;
