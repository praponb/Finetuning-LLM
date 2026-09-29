WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(unit_price) OVER w_warehouse_id_moved_at_month AS warehouse_id_moved_at_month_sum_unit_price,
        ROUND(AVG(unit_price) OVER w_warehouse_id_moved_at_month, 2) AS warehouse_id_moved_at_month_avg_unit_price,
        RANK() OVER (PARTITION BY sku_id, move_type ORDER BY moved_at ASC) AS moved_at_rank,
        ROUND(qty - AVG(qty) OVER w_warehouse_id_moved_at_month, 2) AS qty_vs_warehouse_id_moved_at_month_avg
    FROM base
    WINDOW w_warehouse_id_moved_at_month AS (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at))
),
combined_details_warehouse AS (
    -- Enrich with dimension data
    SELECT s.*, w.wh_code AS warehouse_wh_code
    FROM sequenced s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE UPPER(TRIM(w.wh_code)) LIKE 'K%'
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    qty,
    moved_at,
    unit_price,
    warehouse_id_moved_at_month_sum_unit_price,
    warehouse_id_moved_at_month_avg_unit_price,
    qty_vs_warehouse_id_moved_at_month_avg,
    warehouse_wh_code
FROM combined_details_warehouse
ORDER BY warehouse_wh_code DESC, id NULLS LAST
