WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id IN (1, 2, 6)
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft >= 14888 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
    SIZE(COLLECT_SET(region)) AS num_regions
FROM kept
GROUP BY sqft_tier;
