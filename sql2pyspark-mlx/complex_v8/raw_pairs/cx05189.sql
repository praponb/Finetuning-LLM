WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM stock_moves
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty), 3) AS qty_cume_dist,
        RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty) AS qty_rank,
        SUM(unit_price) OVER (PARTITION BY sku_id) AS sku_id_sum_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty
    FROM scoped_moves
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.move_type_prefix,
        s.rn,
        s.qty_cume_dist,
        s.qty_rank,
        s.sku_id_sum_unit_price,
        s.sku_id_avg_qty,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        sk.is_active AS sku_is_active
    FROM metrics s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY sku_is_active), 2) AS warehouse_capacity_vs_sku_is_active_avg,
        COUNT(*) OVER (PARTITION BY sku_is_active) AS rows_in_sku_is_active
    FROM with_details
),
metrics_3 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(qty_cume_dist / NULLIF(SUM(qty_cume_dist) OVER (PARTITION BY sku_is_active), 0), 4) AS qty_cume_dist_share_of_sku_is_active,
        ROUND(qty_rank / NULLIF(SUM(qty_rank) OVER w_sku_is_active, 0), 4) AS qty_rank_share_of_sku_is_active,
        ROUND(CUME_DIST() OVER (PARTITION BY operator, warehouse_id ORDER BY qty_cume_dist DESC), 3) AS qty_cume_dist_cume_dist,
        LAG(qty, 3) OVER (PARTITION BY warehouse_id, MONTH(warehouse_opened_on) ORDER BY warehouse_opened_on ASC, id) AS prev3_qty
    FROM metrics_2
    WINDOW w_sku_is_active AS (PARTITION BY sku_is_active)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, qty_rank, operator, rows_in_sku_is_active
    FROM metrics_3
) src
PIVOT (
    COUNT(rows_in_sku_is_active)
    FOR operator IN ('op_bee', 'op_chan')
)
ORDER BY warehouse_id, qty_rank
