WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM stock_moves
    WHERE qty <= 357
      AND sku_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        move_type_prefix,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        CASE WHEN unit_price >= 298.9 THEN 'HIGH' WHEN unit_price >= 152.7 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        CONCAT(move_type_prefix, '#', CAST(id AS STRING)) AS move_type_prefix_ref
    FROM filtered
    WHERE move_type <> 'ADJUST'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        unit_price_tier,
        ROUND(AVG(unit_price_bucket), 2) AS avg_unit_price_bucket,
        COUNT(DISTINCT unit_price_tier) AS distinct_unit_price_tiers
    FROM enriched
    GROUP BY sku_id, unit_price_tier
    HAVING AVG(unit_price_bucket) > 126
)
SELECT sku_id, unit_price_tier, distinct_unit_price_tiers
FROM stats
ORDER BY unit_price_tier DESC, sku_id
LIMIT 5
