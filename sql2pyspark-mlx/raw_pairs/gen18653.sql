WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
)
-- Grouped metrics
SELECT
    CASE WHEN sqft >= 12810 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM eligible
GROUP BY CASE WHEN sqft >= 12810 THEN 'HIGH' ELSE 'LOW' END
