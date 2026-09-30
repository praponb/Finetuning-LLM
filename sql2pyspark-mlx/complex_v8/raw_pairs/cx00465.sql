WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE move_type IN ('INBOUND', 'TRANSFER')
      AND warehouse_id >= 3
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_operator_label,
        s.operator_move_type_label,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        sk.sku_code
    FROM base_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER (PARTITION BY sku_id, warehouse_id ORDER BY warehouse_opened_on DESC, id)) AS days_since_prev_warehouse_opened_on,
        ROW_NUMBER() OVER w_warehouse_id_by_moved_at AS rn,
        LEAD(qty, 1, 0) OVER w_warehouse_id_by_moved_at AS next_qty,
        MIN(unit_price) OVER (PARTITION BY sku_id, warehouse_city) AS sku_id_warehouse_city_min_unit_price,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_city), 2) AS qty_vs_warehouse_city_avg,
        MAX(unit_price) OVER w_sku_id_warehouse_city AS sku_id_warehouse_city_max_unit_price,
        LAG(unit_price) OVER w_sku_id_warehouse_id_by_wareh AS prev_unit_price,
        SUM(qty) OVER (PARTITION BY sku_id, warehouse_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        DENSE_RANK() OVER (PARTITION BY warehouse_city, operator ORDER BY moved_at, id) AS moved_at_dense_rank,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_3_prior
    FROM combined_details
    WINDOW w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at ASC, id), w_sku_id_warehouse_city AS (PARTITION BY sku_id, warehouse_city), w_sku_id_warehouse_id_by_wareh AS (PARTITION BY sku_id, warehouse_id ORDER BY warehouse_opened_on DESC, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE rn <= 5
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    move_type_operator_label,
    operator_move_type_label,
    warehouse_opened_on,
    sku_code,
    days_since_prev_warehouse_opened_on,
    rn,
    next_qty,
    sku_id_warehouse_city_min_unit_price,
    qty_vs_warehouse_city_avg,
    sku_id_warehouse_city_max_unit_price,
    prev_unit_price,
    running_qty,
    moved_at_dense_rank,
    qty_avg_3_prior,
    DATE_FORMAT(warehouse_opened_on, 'yyyy-MM-dd') AS warehouse_opened_on_label
FROM top_rows
ORDER BY sku_id_warehouse_city_min_unit_price, id
LIMIT 20
