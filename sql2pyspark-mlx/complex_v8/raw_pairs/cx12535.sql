WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty < 256
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM base_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        warehouse_capacity,
        warehouse_wh_code,
        warehouse_opened_on,
        sku_labels,
        MIN(warehouse_capacity) OVER (PARTITION BY move_type, YEAR(warehouse_opened_on) ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_warehouse_capacity_so_far,
        DENSE_RANK() OVER w_warehouse_id_by_qty AS qty_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY qty ASC), 3) AS qty_cume_dist,
        RANK() OVER w_all_by_warehouse_opened_on AS warehouse_opened_on_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id ORDER BY warehouse_capacity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY sku_is_active, sku_id), 2) AS warehouse_capacity_vs_sku_is_active_sku_id_avg,
        LEAD(unit_price, 3) OVER (PARTITION BY move_type, YEAR(warehouse_opened_on) ORDER BY warehouse_opened_on DESC, id) AS next3_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY move_type ORDER BY warehouse_capacity ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_avg_2
    FROM combined_details_warehouse
    WINDOW w_warehouse_id_by_qty AS (PARTITION BY warehouse_id ORDER BY qty ASC, id), w_all_by_warehouse_opened_on AS (ORDER BY warehouse_opened_on DESC, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(warehouse_opened_on) AS warehouse_opened_on_year,
        warehouse_opened_on_rank,
        qty_dense_rank,
        SUM(warehouse_capacity) AS total_warehouse_capacity,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        MAX(warehouse_opened_on) AS last_warehouse_opened_on,
        ROUND(STDDEV(qty_cume_dist), 2) AS stddev_qty_cume_dist,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(warehouse_wh_code)) >= 6) AS n_length_trim_warehouse_wh_cod,
        COUNT(*) AS n_rows
    FROM windowed
    GROUP BY YEAR(warehouse_opened_on), warehouse_opened_on_rank, qty_dense_rank
)
SELECT
    warehouse_opened_on_year,
    warehouse_opened_on_rank,
    qty_dense_rank,
    total_warehouse_capacity,
    distinct_warehouse_ids,
    last_warehouse_opened_on,
    stddev_qty_cume_dist,
    n_rows
FROM totals
WHERE last_warehouse_opened_on < DATE '2025-07-03'
ORDER BY warehouse_opened_on_year NULLS LAST, last_warehouse_opened_on DESC NULLS FIRST
