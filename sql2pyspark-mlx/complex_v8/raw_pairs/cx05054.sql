WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date,
        CASE WHEN operator IN ('op_eve', 'op_dew') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at_date,
        w.wh_code AS warehouse_wh_code
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.unit_cost AS sku_unit_cost,
        sk.is_active AS sku_is_active,
        w.wh_code AS warehouse_wh_code_2,
        w.capacity AS warehouse_capacity
    FROM joined s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    warehouse_wh_code,
    sku_unit_cost,
    sku_is_active,
    warehouse_capacity,
    CASE WHEN sku_unit_cost >= 320.4 THEN 'large' WHEN sku_unit_cost >= 68.0 THEN 'medium' ELSE 'small' END AS sku_unit_cost_tier
FROM joined_sku
ORDER BY sku_unit_cost DESC NULLS FIRST, move_type DESC NULLS FIRST, id
LIMIT 20
