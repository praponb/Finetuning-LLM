WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.labels AS sku_labels
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE SIZE(sk.labels) >= 1
),
combined_details AS (
    -- Join warehouses
    SELECT
        s.*,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM joined s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(warehouse_capacity) OVER (PARTITION BY operator) AS operator_max_warehouse_capacity,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_city, operator ORDER BY qty DESC), 3) AS qty_cume_dist
    FROM combined_details
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price,
    sku_labels,
    operator_max_warehouse_capacity,
    qty_cume_dist
FROM windowed
WHERE qty <= 349
ORDER BY warehouse_id DESC, sku_id DESC
