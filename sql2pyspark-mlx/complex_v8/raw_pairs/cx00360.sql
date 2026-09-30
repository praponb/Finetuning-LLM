WITH cleaned AS (
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
        MONTH(moved_at) AS moved_at_month,
        ABS(unit_price - 248.3) AS unit_price_distance
    FROM stock_moves
    WHERE unit_price <= 329.7
      AND move_type <> 'OUTBOUND'
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_distance,
        sk.is_active AS sku_is_active,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM cleaned s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg BETWEEN 6.4 AND 21.0
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_type, operator, sku_weight_kg
    FROM combined_details_sku
) src
PIVOT (
    MIN(sku_weight_kg)
    FOR operator IN ('op_ann', 'op_eve')
)
