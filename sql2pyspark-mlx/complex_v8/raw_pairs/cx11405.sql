WITH base_moves AS (
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
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM stock_moves
    WHERE qty > 137
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_plus_30m,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity
    FROM base_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND LENGTH(TRIM(w.wh_code)) >= 4
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        warehouse_city,
        warehouse_capacity,
        LAST_VALUE(warehouse_capacity) IGNORE NULLS OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_capacity,
        FIRST_VALUE(moved_at_plus_30m) OVER (PARTITION BY sku_id, warehouse_city ORDER BY warehouse_capacity ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_plus_30m,
        RANK() OVER (ORDER BY moved_at) AS moved_at_rank,
        DENSE_RANK() OVER w_sku_id_warehouse_city_by_war_2 AS warehouse_capacity_dense_rank,
        unit_price - LAG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY warehouse_capacity DESC, id) AS unit_price_change,
        ROUND(unit_price - AVG(unit_price) OVER w_move_type, 2) AS unit_price_vs_move_type_avg,
        FIRST_VALUE(moved_at_plus_30m) OVER (PARTITION BY sku_id ORDER BY moved_at_plus_30m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_plus_30m_2,
        RANK() OVER (PARTITION BY sku_id ORDER BY moved_at_plus_30m ASC, id) AS moved_at_plus_30m_rank,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY warehouse_capacity, id) AS warehouse_capacity_seq,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id ORDER BY moved_at_plus_30m ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        ROUND(warehouse_capacity / NULLIF(SUM(warehouse_capacity) OVER (PARTITION BY move_type), 0), 4) AS warehouse_capacity_share_of_move_type
    FROM with_details
    WINDOW w_sku_id_warehouse_city_by_war_2 AS (PARTITION BY sku_id, warehouse_city ORDER BY warehouse_capacity ASC), w_move_type AS (PARTITION BY move_type)
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        warehouse_capacity,
        moved_at_rank,
        warehouse_capacity_dense_rank,
        unit_price_vs_move_type_avg,
        moved_at_plus_30m_rank,
        warehouse_capacity_seq,
        SUM(unit_price_change) OVER (PARTITION BY moved_at_plus_30m_rank ORDER BY first_moved_at_plus_30m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_change,
        ROW_NUMBER() OVER w_moved_at_plus_30m_rank_by_fi_2 AS seq_no,
        DENSE_RANK() OVER (ORDER BY warehouse_capacity NULLS LAST, id) AS warehouse_capacity_dense_rank_2,
        DENSE_RANK() OVER w_moved_at_plus_30m_rank_by_un AS unit_price_vs_move_type_avg_dense_rank,
        ROUND(moved_at_rank / NULLIF(SUM(moved_at_rank) OVER w_moved_at_plus_30m_rank, 0), 4) AS moved_at_rank_share_of_moved_at_plus_30m_rank,
        DENSE_RANK() OVER (PARTITION BY moved_at_plus_30m_rank ORDER BY unit_price_vs_move_type_avg ASC) AS unit_price_vs_move_type_avg_dense_rank_2,
        SUM(unit_price_change) OVER (PARTITION BY moved_at_plus_30m_rank) AS moved_at_plus_30m_rank_sum_unit_price_change
    FROM windowed
    WINDOW w_moved_at_plus_30m_rank_by_fi_2 AS (PARTITION BY moved_at_plus_30m_rank ORDER BY first_moved_at_plus_30m DESC, id), w_moved_at_plus_30m_rank_by_un AS (PARTITION BY moved_at_plus_30m_rank ORDER BY unit_price_vs_move_type_avg ASC, id), w_moved_at_plus_30m_rank AS (PARTITION BY moved_at_plus_30m_rank)
)
-- Final projection
SELECT
    id,
    sku_id,
    warehouse_capacity,
    moved_at_rank,
    warehouse_capacity_dense_rank,
    running_unit_price_change,
    warehouse_capacity_dense_rank_2,
    unit_price_vs_move_type_avg_dense_rank,
    moved_at_rank_share_of_moved_at_plus_30m_rank,
    unit_price_vs_move_type_avg_dense_rank_2,
    moved_at_plus_30m_rank_sum_unit_price_change
FROM windowed_2
ORDER BY moved_at_plus_30m_rank_sum_unit_price_change DESC, unit_price_vs_move_type_avg_dense_rank_2 DESC NULLS FIRST
