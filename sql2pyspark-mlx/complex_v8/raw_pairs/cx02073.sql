WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
latest AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty DESC, id DESC) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        qty - LAG(qty) OVER (PARTITION BY sku_id ORDER BY qty, id) AS qty_change,
        MIN(qty) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_min_qty
    FROM latest
),
joined AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.warehouse_id_sku_id_min_qty,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM sequenced s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.opened_on >= DATE '2024-11-25'
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.warehouse_id_sku_id_min_qty,
        s.warehouse_city,
        s.warehouse_opened_on,
        w.opened_on AS warehouse_opened_on_2,
        w.capacity AS warehouse_capacity
    FROM joined s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    id,
    sku_id,
    warehouse_id_sku_id_min_qty,
    warehouse_city,
    warehouse_opened_on,
    warehouse_capacity,
    DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
    FLOOR(warehouse_capacity / 5000) * 5000 AS warehouse_capacity_bucket
FROM joined_warehouse
ORDER BY warehouse_opened_on DESC, id DESC
