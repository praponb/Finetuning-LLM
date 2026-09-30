WITH src AS (
    SELECT *
    FROM stores
    WHERE city = 'Chiang Mai'
),
agg AS (
    SELECT CASE WHEN sqft >= 15473 THEN 'high' WHEN sqft >= 6455 THEN 'mid' ELSE 'low' END AS sqft_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM src
    GROUP BY CASE WHEN sqft >= 15473 THEN 'high' WHEN sqft >= 6455 THEN 'mid' ELSE 'low' END
)
SELECT sqft_bucket, city_values
FROM agg
ORDER BY 1
LIMIT 3
