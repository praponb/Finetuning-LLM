SELECT
    city,
    CASE WHEN sqft >= 16329 THEN 'high' WHEN sqft >= 8146 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
    MIN(sqft) AS min_sqft,
    ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
GROUP BY 1, 2
ORDER BY tokyo_count DESC
