WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_per_qty,
        LOWER(TRIM(move_type)) AS move_type_clean,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        LOWER(TRIM(move_type)) AS move_type_clean_2
    FROM valid_moves
),
combined_details_sku AS (
    -- Join skus
    SELECT s.*, sk.weight_kg AS sku_weight_kg
    FROM enriched s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price_per_qty,
    move_type_clean,
    operator_prefix,
    CASE WHEN unit_price >= 307.8 THEN 'large' WHEN unit_price >= 238.5 THEN 'medium' ELSE 'small' END AS unit_price_tier,
    LENGTH(TRIM(move_type_clean_2)) AS move_type_clean_2_len
FROM combined_details_sku
ORDER BY unit_price_tier DESC
