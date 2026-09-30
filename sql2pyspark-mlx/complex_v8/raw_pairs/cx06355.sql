WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year,
        CASE WHEN move_type IN ('OUTBOUND', 'ADJUST') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE warehouse_id <= 5
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.move_type_len,
        s.moved_at_year,
        s.move_type_group,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM base s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_details s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE TO_DATE(st.moved_at) < DATE '2025-02-18'
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.move_type_len,
        s.moved_at_year,
        s.move_type_group,
        s.warehouse_wh_code,
        w.capacity AS warehouse_capacity_2,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code_2
    FROM filtered s
    JOIN warehouses w ON w.id = s.warehouse_id
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_type_len,
        s.move_type_group,
        s.warehouse_wh_code,
        s.warehouse_capacity_2,
        s.warehouse_wh_code_2,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty
    FROM joined s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id AND st.move_type NOT IN ('INBOUND', 'ADJUST')
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_type_group,
        s.warehouse_wh_code,
        s.warehouse_wh_code_2,
        s.move_id,
        s.move_move_type,
        s.move_warehouse_id,
        s.move_qty,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost
    FROM line_level_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, field, amount
FROM (
    SELECT id, move_id, sku_weight_kg, sku_unit_cost
    FROM enriched
) src
UNPIVOT (
    amount FOR field IN (sku_weight_kg, sku_unit_cost)
) unp
