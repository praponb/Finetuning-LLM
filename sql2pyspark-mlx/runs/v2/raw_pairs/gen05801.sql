WITH filtered_stores AS (
    SELECT *, COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE LENGTH(store_name) > 7 AND sqft <= 5994
),
stores_stats AS (
    SELECT city, ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM filtered_stores
    GROUP BY city
)
SELECT city, stddev_sqft
FROM stores_stats
WHERE stddev_sqft < 78
ORDER BY city ASC
