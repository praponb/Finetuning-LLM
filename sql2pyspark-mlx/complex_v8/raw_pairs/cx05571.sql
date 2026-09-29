WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM stock_moves
    WHERE (operator = 'op_chan' OR warehouse_id <= 4)
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.moved_at_plus_30m,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM valid_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        unit_price - LAG(unit_price) OVER (ORDER BY warehouse_capacity, id) AS unit_price_change,
        MAX(unit_price) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price,
        RANK() OVER w_sku_is_active_by_unit_price AS unit_price_rank,
        ROUND(CUME_DIST() OVER w_sku_is_active_by_unit_price, 3) AS unit_price_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(moved_at_plus_30m) ORDER BY moved_at_plus_30m DESC), 3) AS moved_at_plus_30m_cume_dist,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER w_warehouse_city_warehouse_id, 2) AS warehouse_capacity_vs_warehouse_city_warehouse_i,
        MAX(warehouse_capacity) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at_plus_30m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_warehouse_capacity_so_far,
        ROUND(AVG(warehouse_capacity) OVER (ORDER BY warehouse_capacity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS warehouse_capacity_avg_6_prior,
        ROUND(CUME_DIST() OVER w_moved_at_plus_30m_year_by_mo_2, 3) AS moved_at_plus_30m_cume_dist_2,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_unit_price,
        MIN(warehouse_capacity) OVER (PARTITION BY warehouse_city, warehouse_id) AS warehouse_city_warehouse_id_min_warehouse_capaci,
        MAX(warehouse_capacity) OVER (PARTITION BY YEAR(moved_at_plus_30m) ORDER BY moved_at_plus_30m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_warehouse_capacity_so_far_2,
        NTILE(3) OVER w_moved_at_plus_30m_year_by_mo_2 AS moved_at_plus_30m_tercile,
        DENSE_RANK() OVER w_moved_at_plus_30m_year_by_mo_2 AS moved_at_plus_30m_dense_rank
    FROM combined_details
    WINDOW w_sku_is_active_by_unit_price AS (PARTITION BY sku_is_active ORDER BY unit_price), w_warehouse_city_warehouse_id AS (PARTITION BY warehouse_city, warehouse_id), w_moved_at_plus_30m_year_by_mo_2 AS (PARTITION BY YEAR(moved_at_plus_30m) ORDER BY moved_at_plus_30m DESC, id)
)
-- Final projection
SELECT
    id,
    sku_id,
    unit_price,
    moved_at_plus_30m,
    warehouse_capacity,
    sku_labels,
    sku_is_active,
    sku_id_max_unit_price,
    unit_price_rank,
    unit_price_cume_dist,
    moved_at_plus_30m_cume_dist,
    warehouse_capacity_vs_warehouse_city_warehouse_i,
    max_warehouse_capacity_so_far,
    moved_at_plus_30m_cume_dist_2,
    sku_id_avg_unit_price,
    warehouse_city_warehouse_id_min_warehouse_capaci,
    max_warehouse_capacity_so_far_2,
    moved_at_plus_30m_tercile,
    moved_at_plus_30m_dense_rank
FROM metrics
