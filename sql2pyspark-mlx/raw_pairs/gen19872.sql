SELECT
    region,
    ROUND(AVG(sqft), 2) avg_sqft,
    COUNT(*) cnt,
    MAX(sqft) max_sqft
FROM stores
GROUP BY region
HAVING MAX(sqft) >= 13800
ORDER BY max_sqft DESC, region
LIMIT 20
