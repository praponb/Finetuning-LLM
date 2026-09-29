WITH filtered_moves AS (
    -- Scope the stock_moves rows
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
    WHERE move_type IS NOT NULL
      AND sku_id >= 12
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.is_active AS sku_is_active
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_warehouse_city_sku_id_by_qty AS qty_rank,
        ROUND(qty - AVG(qty) OVER w_move_type_warehouse_id, 2) AS qty_vs_move_type_warehouse_id_avg
    FROM combined_details
    WINDOW w_warehouse_city_sku_id_by_qty AS (PARTITION BY warehouse_city, sku_id ORDER BY qty ASC), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(moved_at) AS moved_at_date,
        FLOOR(qty_rank / 2) * 2 AS qty_rank_bucket
    FROM with_windows
    WHERE qty_rank BETWEEN 2 AND 10
)
SELECT *
FROM (
    SELECT sku_is_active, moved_at_date, warehouse_city, qty_vs_move_type_warehouse_id_avg
    FROM tagged
) src
PIVOT (
    SUM(qty_vs_move_type_warehouse_id_avg)
    FOR warehouse_city IN ('Bangkok', 'Singapore')
)
ORDER BY sku_is_active, moved_at_date
