WITH recent_stores AS (
    SELECT *, INITCAP(TRIM(region)) AS region_title
    FROM stores
    WHERE city IN ('London', 'Tokyo')
),
stores_stats AS (
    SELECT city, CASE WHEN region IN ('West', 'Central') THEN 'group_a' ELSE 'other' END AS region_group, SUM(CASE WHEN city = 'Chiang Mai' THEN sqft ELSE 0 END) AS chiang_mai_sqft
    FROM recent_stores
    GROUP BY city, CASE WHEN region IN ('West', 'Central') THEN 'group_a' ELSE 'other' END
)
SELECT city, region_group, chiang_mai_sqft
FROM stores_stats
ORDER BY region_group
