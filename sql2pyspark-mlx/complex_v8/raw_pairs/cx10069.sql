WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE warehouse_id <= 5
),
with_parent AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        sk.unit_cost AS sku_unit_cost,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.unit_cost >= 134.3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        sku_unit_cost,
        warehouse_wh_code,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price), 3) AS unit_price_cume_dist,
        FIRST_VALUE(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        LEAD(qty) OVER w_warehouse_id_sku_id_by_unit_3 AS next_qty
    FROM with_parent
    WINDOW w_warehouse_id_sku_id_by_unit_3 AS (PARTITION BY warehouse_id, sku_id ORDER BY unit_price, id)
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS max_move_qty,
        (SELECT COUNT(CASE WHEN st.operator = 'op_dew' THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM with_windows s
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        warehouse_wh_code,
        unit_price_cume_dist,
        next_qty,
        max_move_qty,
        move_operator_count,
        move_count,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, move_count ORDER BY max_move_qty DESC), 3) AS max_move_qty_cume_dist,
        LEAD(first_unit_price) OVER (PARTITION BY sku_id, move_count ORDER BY max_move_qty DESC, id) AS next_first_unit_price,
        DENSE_RANK() OVER (PARTITION BY sku_id, move_count ORDER BY max_move_qty DESC) AS max_move_qty_dense_rank,
        MIN(move_operator_count) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_move_operator_count
    FROM summarized_rows
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(warehouse_wh_code, '#', CAST(move_count AS STRING)) AS warehouse_wh_code_ref
    FROM ranked
)
-- Final projection
SELECT
    id,
    warehouse_id,
    warehouse_wh_code,
    unit_price_cume_dist,
    next_qty,
    max_move_qty,
    move_operator_count,
    move_count,
    max_move_qty_cume_dist,
    max_move_qty_dense_rank,
    warehouse_wh_code_ref,
    CASE WHEN max_move_qty_cume_dist >= 0.8 THEN 'A' WHEN max_move_qty_cume_dist >= 0.2 THEN 'B' ELSE 'C' END AS max_move_qty_cume_dist_tier,
    CONCAT(warehouse_wh_code_ref, '#', CAST(next_qty AS STRING)) AS warehouse_wh_code_ref_ref
FROM derived
ORDER BY id DESC
