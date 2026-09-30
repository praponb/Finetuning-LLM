WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE (sku_id <= 4 OR move_type = 'OUTBOUND')
),
screened AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM valid_moves s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.capacity < 11506
              AND w.city = 'Kuala Lumpur'
        )
),
with_parent_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.move_type_clean,
        w.wh_code AS warehouse_wh_code,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg
    FROM screened s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity BETWEEN 2614 AND 11768
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    move_type_clean,
    warehouse_wh_code,
    sku_unit_cost,
    sku_weight_kg,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM with_parent_warehouse
ORDER BY operator_slug, id DESC
