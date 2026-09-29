WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE qty <= 326
),
without_matches AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM scoped_moves s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.capacity >= 4912
              AND UPPER(TRIM(w.wh_code)) LIKE 'B%'
        )
),
with_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels
    FROM without_matches s
    JOIN skus sk ON sk.id = s.sku_id AND UPPER(TRIM(sk.sku_code)) LIKE 'E%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(qty - AVG(qty) OVER w_warehouse_id_sku_is_active, 2) AS qty_vs_warehouse_id_sku_is_active_avg,
        COUNT(*) OVER w_warehouse_id_sku_is_active AS rows_in_warehouse_id_sku_is_active,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, sku_is_active), 2) AS warehouse_id_sku_is_active_avg_qty,
        ROUND(CUME_DIST() OVER (ORDER BY qty), 3) AS qty_cume_dist
    FROM with_details_sku
    WINDOW w_warehouse_id_sku_is_active AS (PARTITION BY warehouse_id, sku_is_active)
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.sku_labels,
        s.qty_vs_warehouse_id_sku_is_active_avg,
        s.rows_in_warehouse_id_sku_is_active,
        s.qty_cume_dist,
        w.capacity AS warehouse_capacity
    FROM sequenced s
    JOIN warehouses w ON w.id = s.warehouse_id
),
without_matches_2 AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IN ('op_bee', 'op_eve')
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, operator, qty_cume_dist
    FROM without_matches_2
) src
PIVOT (
    MIN(qty_cume_dist)
    FOR operator IN ('op_ann', 'op_bee', 'op_dew', 'op_eve')
)
ORDER BY sku_id
