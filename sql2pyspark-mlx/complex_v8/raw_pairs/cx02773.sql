WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        CASE WHEN move_type IN ('ADJUST', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
enriched_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.qty_per_unit_price,
        s.move_type_group,
        s.moved_at_day,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM scoped_moves s
    JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg >= 6.4
),
combined_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.qty_per_unit_price,
        s.move_type_group,
        s.moved_at_day,
        s.sku_code,
        s.sku_unit_cost,
        sk.unit_cost AS sku_unit_cost_2,
        sk.sku_code AS sku_sku_code
    FROM enriched_sku s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    sku_id,
    qty,
    qty_per_unit_price,
    move_type_group,
    moved_at_day,
    sku_code,
    sku_unit_cost,
    COALESCE(qty, 0) AS qty_filled,
    CASE WHEN move_type_group IN ('group_a', 'group_b') THEN 'group_a' WHEN move_type_group = 'other' THEN 'group_b' ELSE 'other' END AS move_type_group_group
FROM combined_details_sku
ORDER BY qty DESC NULLS FIRST
