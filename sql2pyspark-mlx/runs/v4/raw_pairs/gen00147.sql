WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        items,
        attributes,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        SIZE(items) AS num_items
    FROM carts
    WHERE created_at >= '2024-12-01'
)
-- Grouped metrics
SELECT
    CASE WHEN weight_kg >= 4.1 THEN 'PREMIUM' ELSE 'STANDARD' END AS weight_kg_tier,
    CASE WHEN num_items >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END AS num_items_tier,
    ROUND(AVG(num_items), 2) AS avg_num_items,
    MAX(created_at) AS last_created_at,
    MAX(created_at) AS last_seen
FROM parsed
GROUP BY 1, 2
HAVING ROUND(AVG(num_items), 2) >= 0
ORDER BY weight_kg_tier
