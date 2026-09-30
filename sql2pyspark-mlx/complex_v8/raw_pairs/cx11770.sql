WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        qty - unit_price AS qty_minus_unit_price,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR qty >= 138)
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_len,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM valid_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(operator_len) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_min_operator_len,
        FIRST_VALUE(warehouse_capacity) IGNORE NULLS OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_warehouse_capacity,
        MAX(warehouse_capacity) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_max_warehouse_capacity,
        ROUND(operator_len - AVG(operator_len) OVER (PARTITION BY warehouse_id, sku_id), 2) AS operator_len_vs_warehouse_id_sku_id_avg,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY MONTH(moved_at), operator ORDER BY moved_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_2,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_operator_len_by_warehouse_ca)) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id, sku_id), 2) AS qty_vs_warehouse_id_sku_id_avg,
        LAG(qty) OVER w_all_by_qty AS prev_qty,
        ROUND(operator_len - AVG(operator_len) OVER (PARTITION BY warehouse_city, warehouse_id), 2) AS operator_len_vs_warehouse_city_warehouse_id_avg,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY warehouse_capacity) AS warehouse_capacity_dense_rank,
        COUNT(*) OVER w_sku_id_moved_at_month AS rows_in_sku_id_moved_at_month,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_city, warehouse_id), 2) AS warehouse_city_warehouse_id_avg_unit_price,
        ROUND(AVG(operator_len) OVER (PARTITION BY MONTH(moved_at), operator ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS operator_len_avg_3_prior,
        warehouse_capacity - LAG(warehouse_capacity) OVER (ORDER BY qty DESC, id) AS warehouse_capacity_change
    FROM with_details_warehouse
    WINDOW w_operator_len_by_warehouse_ca AS (PARTITION BY operator_len ORDER BY warehouse_capacity DESC, id), w_all_by_qty AS (ORDER BY qty DESC, id), w_sku_id_moved_at_month AS (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at))
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE warehouse_capacity_dense_rank <= 3
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator_len,
    warehouse_city,
    warehouse_capacity,
    warehouse_wh_code,
    warehouse_id_sku_id_min_operator_len,
    warehouse_id_sku_id_max_warehouse_capacity,
    operator_len_vs_warehouse_id_sku_id_avg,
    minutes_since_prev_moved_at,
    qty_vs_warehouse_id_sku_id_avg,
    warehouse_capacity_dense_rank,
    rows_in_sku_id_moved_at_month,
    operator_len_avg_3_prior,
    warehouse_capacity_change
FROM top_rows
ORDER BY qty_vs_warehouse_id_sku_id_avg, operator_len DESC
