WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        unit_price + qty AS unit_price_plus_qty,
        CASE WHEN operator IN ('op_bee', 'op_chan') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE unit_price < 367.7
      AND sku_id >= 15
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        ABS(unit_price_plus_qty - 400.6) AS unit_price_plus_qty_distance
    FROM prepared_moves
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.unit_price_plus_qty,
        s.unit_price_plus_qty_distance,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id
    FROM scored s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.unit_price_plus_qty,
        s.move_id,
        s.move_qty,
        s.move_sku_id,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        st.unit_price AS move_unit_price
    FROM line_level_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.opened_on IS NOT NULL
    LEFT JOIN stock_moves st ON st.id = s.move_id
    WHERE w.opened_on < DATE '2025-02-14'
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    move_qty,
    move_sku_id,
    warehouse_wh_code,
    warehouse_city,
    warehouse_opened_on,
    move_unit_price,
    CASE WHEN move_unit_price >= 341.5 THEN 'large' WHEN move_unit_price >= 119.5 THEN 'medium' ELSE 'small' END AS move_unit_price_tier
FROM with_details
