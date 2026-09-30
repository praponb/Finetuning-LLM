WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE unit_price > 189.4
      AND warehouse_id IS NOT NULL
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MAX(qty) AS max_move_qty,
        MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price,
        st.max_move_qty,
        st.first_moved_at
    FROM cleaned s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(avg_move_unit_price) OVER (ORDER BY max_move_qty ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_move_unit_price_max_2,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id,
        RANK() OVER w_all_by_max_move_qty AS max_move_qty_rank,
        LAG(moved_at) OVER (ORDER BY max_move_qty ASC, id) AS prev_moved_at,
        ROUND(avg_move_unit_price - AVG(avg_move_unit_price) OVER (PARTITION BY warehouse_id, sku_id), 2) AS avg_move_unit_price_vs_warehouse_id_sku_id_avg
    FROM summarized_rows
    WINDOW w_all_by_max_move_qty AS (ORDER BY max_move_qty ASC, id)
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.avg_move_unit_price,
        s.max_move_qty,
        s.avg_move_unit_price_max_2,
        s.rows_in_warehouse_id_sku_id,
        s.avg_move_unit_price_vs_warehouse_id_sku_id_avg,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.labels AS sku_labels
    FROM metrics s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.city = 'Singapore'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, operator, warehouse_city, avg_move_unit_price_vs_warehouse_id_sku_id_avg
    FROM with_details_warehouse
) src
PIVOT (
    SUM(avg_move_unit_price_vs_warehouse_id_sku_id_avg) AS total, MAX(avg_move_unit_price_vs_warehouse_id_sku_id_avg) AS max_v
    FOR warehouse_city IN ('Singapore', 'Bangkok')
)
