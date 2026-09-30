WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE sku_id <= 3
      AND warehouse_id >= 4
),
with_parent AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM base s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND LOWER(w.wh_code) LIKE '%l1%'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(qty_per_unit_price - AVG(qty_per_unit_price) OVER w_warehouse_city, 2) AS qty_per_unit_price_vs_warehouse_city_avg,
        LAST_VALUE(warehouse_opened_on) OVER (PARTITION BY move_type ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_warehouse_opened_on,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_7,
        SIZE(COLLECT_SET(warehouse_city) OVER (PARTITION BY warehouse_id)) AS distinct_warehouse_city_in_warehouse_id,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_city)) AS distinct_move_type_in_warehouse_city,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER (ORDER BY unit_price NULLS LAST, id)) AS days_since_prev_warehouse_opened_on,
        ROUND(unit_price - AVG(unit_price) OVER w_warehouse_id, 2) AS unit_price_vs_warehouse_id_avg,
        FIRST_VALUE(qty) IGNORE NULLS OVER (ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        DENSE_RANK() OVER w_all_by_qty_per_unit_price_ti AS qty_per_unit_price_dense_rank,
        COUNT(*) OVER (PARTITION BY warehouse_city) AS rows_in_warehouse_city,
        LAST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_opened_on,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_warehouse_id, 0), 4) AS unit_price_share_of_warehouse_id
    FROM with_parent
    WINDOW w_warehouse_city AS (PARTITION BY warehouse_city), w_warehouse_id AS (PARTITION BY warehouse_id), w_all_by_qty_per_unit_price_ti AS (ORDER BY qty_per_unit_price DESC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        unit_price,
        qty_per_unit_price,
        warehouse_capacity,
        warehouse_city,
        qty_per_unit_price_vs_warehouse_city_avg,
        last_warehouse_opened_on,
        warehouse_capacity_avg_7,
        distinct_warehouse_city_in_warehouse_id,
        distinct_move_type_in_warehouse_city,
        first_known_qty,
        qty_per_unit_price_dense_rank,
        rows_in_warehouse_city,
        last_known_warehouse_opened_on,
        unit_price_share_of_warehouse_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_city, sku_id ORDER BY qty_per_unit_price_dense_rank ASC), 3) AS qty_per_unit_price_dense_rank_percent_rank,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        ROUND(first_known_qty - AVG(first_known_qty) OVER (PARTITION BY qty_per_unit_price_dense_rank), 2) AS first_known_qty_vs_qty_per_unit_price_dense_rank,
        NTILE(3) OVER (ORDER BY unit_price_vs_warehouse_id_avg DESC, id) AS unit_price_vs_warehouse_id_avg_tercile,
        LAG(last_warehouse_opened_on, 3) OVER w_warehouse_city_sku_id_by_qty_2 AS prev3_last_warehouse_opened_on,
        NTILE(10) OVER (PARTITION BY warehouse_city, qty_per_unit_price_dense_rank ORDER BY distinct_move_type_in_warehouse_city DESC, id) AS distinct_move_type_in_warehouse_city_decile,
        ROUND(AVG(rows_in_warehouse_city) OVER (ORDER BY unit_price_vs_warehouse_id_avg DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rows_in_warehouse_city_avg_2,
        MIN(unit_price) OVER (PARTITION BY warehouse_city, sku_id ORDER BY qty_per_unit_price_dense_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far
    FROM with_trends
    WINDOW w_warehouse_city_sku_id_by_qty_2 AS (PARTITION BY warehouse_city, sku_id ORDER BY qty_per_unit_price_dense_rank ASC, id)
)
SELECT
    id,
    move_type,
    unit_price,
    qty_per_unit_price,
    warehouse_capacity,
    warehouse_city,
    qty_per_unit_price_vs_warehouse_city_avg,
    last_warehouse_opened_on,
    warehouse_capacity_avg_7,
    distinct_warehouse_city_in_warehouse_id,
    distinct_move_type_in_warehouse_city,
    qty_per_unit_price_dense_rank,
    rows_in_warehouse_city,
    last_known_warehouse_opened_on,
    unit_price_share_of_warehouse_id,
    rows_in_sku_id,
    prev3_last_warehouse_opened_on,
    distinct_move_type_in_warehouse_city_decile,
    rows_in_warehouse_city_avg_2,
    min_unit_price_so_far,
    LENGTH(TRIM(warehouse_city)) AS warehouse_city_len
FROM metrics
ORDER BY distinct_warehouse_city_in_warehouse_id, id NULLS LAST
