WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE (sku_id >= 17 OR qty >= 123)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(move_type)) AS move_type_len,
        CASE WHEN unit_price >= 298.9 THEN 'HIGH' WHEN unit_price >= 161.4 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM scoped
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT s.*, sk.category AS sku_category, sk.unit_cost AS sku_unit_cost
    FROM scored s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Subtotals with GROUPING SETS
SELECT
    sku_category,
    sku_id,
    move_type,
    GROUPING(sku_category) AS is_total_sku_category,
    ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
FROM with_details_sku
GROUP BY GROUPING SETS ((sku_category, sku_id, move_type), (sku_category), (sku_id))
