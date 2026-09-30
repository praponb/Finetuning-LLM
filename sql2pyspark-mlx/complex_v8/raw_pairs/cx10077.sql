WITH base AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE unit_cost > 134.3
      AND NOT is_active
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown,
        ABS(unit_cost - 239.3) AS unit_cost_distance
    FROM base
    WHERE weight_kg <= 20.9
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.move_type IN ('INBOUND', 'OUTBOUND', 'TRANSFER') THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.id) AS move_move_type_count
    FROM enriched s
)
-- Final projection
SELECT
    id,
    sku_code,
    category,
    unit_cost,
    weight_kg,
    labels,
    is_active,
    category_or_unknown,
    unit_cost_distance,
    ROUND(unit_cost / NULLIF(unit_cost_distance, 0), 3) AS unit_cost_per_unit_cost_distance,
    LENGTH(TRIM(sku_code)) AS sku_code_len
FROM summarized_rows
ORDER BY category_or_unknown NULLS LAST, unit_cost DESC NULLS FIRST
