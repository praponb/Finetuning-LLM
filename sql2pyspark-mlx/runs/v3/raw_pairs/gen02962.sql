SELECT *
FROM (
    SELECT
        region,
        city,
        MAX(sqft) max_sqft,
        AVG(sqft) avg_sqft
    FROM stores
    GROUP BY region, city
) x
WHERE avg_sqft >= 4800
ORDER BY max_sqft, avg_sqft
