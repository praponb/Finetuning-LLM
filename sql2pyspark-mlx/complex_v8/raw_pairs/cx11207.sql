WITH recent AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN unit_cost >= 306.4 THEN 'HIGH' WHEN unit_cost >= 108.5 THEN 'MEDIUM' ELSE 'LOW' END AS unit_cost_tier
    FROM skus
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        COUNT(*) AS move_count_2,
        COUNT(*) AS move_count_3
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2,
        COALESCE(st.move_count_3, 0) AS move_count_3
    FROM recent s
    LEFT JOIN move_stats st ON st.sku_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, unit_cost, weight_kg
    FROM with_counts
) src
UNPIVOT (
    reading FOR measure IN (unit_cost, weight_kg)
) unp
ORDER BY id, measure
