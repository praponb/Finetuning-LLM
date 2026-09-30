WITH valid_stores AS (
    SELECT *
    FROM stores
    WHERE store_name IS NOT NULL
),
agg AS (
    SELECT CASE WHEN city IN ('London', 'Berlin') THEN 'group_a' ELSE 'other' END AS city_group, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values, ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM valid_stores
    GROUP BY CASE WHEN city IN ('London', 'Berlin') THEN 'group_a' ELSE 'other' END
)
SELECT *
FROM agg
WHERE stddev_sqft >= 44
ORDER BY city_group DESC
