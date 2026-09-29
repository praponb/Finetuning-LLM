WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator
    FROM stock_moves
),
unique_rows AS (
    -- Keep the latest row per warehouse_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, move_type ORDER BY qty DESC, id DESC) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code
    FROM unique_rows s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    operator,
    warehouse_city,
    warehouse_wh_code,
    COALESCE(warehouse_city, 'UNKNOWN') AS warehouse_city_or_unknown
FROM with_details
ORDER BY warehouse_city, move_type
