WITH base AS (
    -- Scope the skus rows
    SELECT
        id,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN unit_cost >= 251.2 THEN 'A' WHEN unit_cost >= 164.5 THEN 'B' ELSE 'C' END AS unit_cost_tier
    FROM skus
    WHERE (unit_cost < 277.0 OR SIZE(labels) >= 1)
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) FILTER (WHERE operator IS NOT NULL) AS move_operator_count,
        MAX(unit_price) AS max_move_unit_price
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count, 0) AS move_operator_count,
        st.max_move_unit_price
    FROM base s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM summarized_rows
    WHERE max_move_unit_price > (SELECT MIN(max_move_unit_price) FROM summarized_rows)
)
SELECT
    id,
    unit_cost,
    weight_kg,
    is_active,
    unit_cost_tier,
    move_operator_count,
    max_move_unit_price,
    LOWER(REGEXP_REPLACE(TRIM(unit_cost_tier), ' ', '_')) AS unit_cost_tier_slug
FROM filtered
ORDER BY move_operator_count DESC
