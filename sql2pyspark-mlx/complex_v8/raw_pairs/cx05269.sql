WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label
    FROM stock_moves
    WHERE (move_type IN ('ADJUST', 'TRANSFER') OR warehouse_id IS NOT NULL)
),
deduped AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        moved_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id DESC) AS rn
        FROM filtered_moves
    ) ranked
    WHERE rn = 1
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT s.*, w.wh_code AS warehouse_wh_code, w.city AS warehouse_city
    FROM deduped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.moved_at_label,
        s.warehouse_city,
        w.city AS warehouse_city_2,
        w.capacity AS warehouse_capacity
    FROM with_details_warehouse s
    JOIN warehouses w ON w.id = s.warehouse_id
),
latest AS (
    -- Keep the earliest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        moved_at_label,
        warehouse_city,
        warehouse_city_2,
        warehouse_capacity
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty, id) AS recency_rank
        FROM with_details
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    moved_at_label,
    warehouse_city,
    warehouse_city_2,
    warehouse_capacity,
    LEAST(qty, warehouse_capacity) AS least_qty_warehouse_capacity
FROM latest
ORDER BY sku_id DESC NULLS FIRST, moved_at_label NULLS LAST
