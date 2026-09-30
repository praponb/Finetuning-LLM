WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 275) AS qty_distance,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE qty >= 138
      AND TO_DATE(moved_at) < DATE '2025-02-24'
),
combined_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        sk.labels AS sku_labels,
        sk.sku_code
    FROM valid_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY warehouse_opened_on, id) AS warehouse_opened_on_quintile,
        unit_price - LAG(unit_price) OVER (ORDER BY qty_distance DESC, id) AS unit_price_change,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_city), 2) AS unit_price_vs_warehouse_city_avg,
        NTILE(10) OVER w_move_type_len_warehouse_open AS qty_distance_decile,
        MAX(warehouse_capacity) OVER w_warehouse_city AS warehouse_city_max_warehouse_capacity,
        COUNT(*) OVER (PARTITION BY warehouse_city) AS rows_in_warehouse_city,
        ROUND(AVG(move_type_len) OVER (ORDER BY move_type_len, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS move_type_len_avg_2,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER (ORDER BY move_type_len, id)) AS days_since_prev_warehouse_opened_on,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_city)) AS distinct_operator_in_warehouse_city,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS unit_price_vs_warehouse_id_avg,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_id), 0), 4) AS unit_price_share_of_warehouse_id,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        MIN(qty_distance) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_qty_distance
    FROM combined_details_warehouse
    WINDOW w_move_type_len_warehouse_open AS (PARTITION BY move_type_len, YEAR(warehouse_opened_on) ORDER BY qty_distance DESC, id), w_warehouse_city AS (PARTITION BY warehouse_city), w_warehouse_id AS (PARTITION BY warehouse_id)
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(qty_distance_decile) OVER (PARTITION BY sku_id, distinct_sku_id_in_warehouse_id ORDER BY warehouse_city_max_warehouse_capacity, id) AS next_qty_distance_decile,
        MIN(unit_price_change) OVER (PARTITION BY sku_id, distinct_sku_id_in_warehouse_id) AS sku_id_distinct_sku_id_in_warehouse_id_min_unit_,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_operator_in_warehouse_city ORDER BY warehouse_id_min_qty_distance, id), 3) AS warehouse_id_min_qty_distance_percent_rank,
        NTH_VALUE(warehouse_opened_on_quintile, 2) OVER (PARTITION BY sku_id, distinct_sku_id_in_warehouse_id ORDER BY warehouse_city_max_warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on_quintile,
        SUM(warehouse_capacity) OVER (PARTITION BY sku_id, distinct_sku_id_in_warehouse_id) AS sku_id_distinct_sku_id_in_warehouse_id_sum_wareh,
        COUNT(*) OVER (PARTITION BY sku_id, distinct_sku_id_in_warehouse_id) AS rows_in_sku_id_distinct_sku_id_in_warehouse_id,
        MIN(qty_distance_decile) OVER w_sku_id_distinct_sku_id_in_wa_2 AS sku_id_distinct_sku_id_in_warehouse_id_min_qty_d,
        NTILE(10) OVER (PARTITION BY sku_id, distinct_sku_id_in_warehouse_id ORDER BY warehouse_city_max_warehouse_capacity, id) AS warehouse_city_max_warehouse_capacity_decile
    FROM with_trends
    WINDOW w_sku_id_distinct_sku_id_in_wa_2 AS (PARTITION BY sku_id, distinct_sku_id_in_warehouse_id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        qty_distance_decile,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_ym,
        SUM(unit_price) AS total_unit_price,
        ROUND(SUM(unit_price_change) / NULLIF(SUM(qty), 0), 3) AS unit_price_change_to_qty_ratio,
        SUM(CASE WHEN warehouse_city IN ('Kuala Lumpur', 'Bangkok') THEN 1 ELSE 0 END) AS n_warehouse_city_in_kuala_lump,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        MAX(move_type_len_avg_2) AS max_move_type_len_avg_2
    FROM with_trends_2
    WHERE sku_id_distinct_sku_id_in_warehouse_id_min_unit_ <= 2800.0
    GROUP BY qty_distance_decile, DATE_FORMAT(moved_at, 'yyyy-MM')
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    qty_distance_decile,
    total_unit_price,
    unit_price_change_to_qty_ratio,
    n_warehouse_city_in_kuala_lump,
    n_rows,
    max_move_type_len_avg_2,
    LENGTH(TRIM(moved_at_ym)) AS moved_at_ym_len
FROM rollup_base
ORDER BY total_unit_price DESC
