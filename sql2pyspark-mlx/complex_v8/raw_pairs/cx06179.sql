WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty > 191
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        w.wh_code AS warehouse_wh_code,
        sk.unit_cost AS sku_unit_cost
    FROM cleaned_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        warehouse_wh_code,
        sku_unit_cost,
        DENSE_RANK() OVER w_sku_id_by_sku_unit_cost_ties AS sku_unit_cost_dense_rank,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_id, sku_id), 0), 4) AS unit_price_share_of_warehouse_id_sku_id,
        ROUND(AVG(sku_unit_cost) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS sku_unit_cost_avg_3,
        COUNT(*) OVER w_warehouse_id_move_type AS rows_in_warehouse_id_move_type,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id) AS unit_price_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price DESC, id), 3) AS unit_price_cume_dist,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, move_type), 2) AS unit_price_vs_warehouse_id_move_type_avg,
        LAST_VALUE(unit_price) OVER (PARTITION BY sku_id ORDER BY sku_unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY unit_price ASC) AS unit_price_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type ORDER BY unit_price DESC NULLS FIRST), 3) AS unit_price_percent_rank,
        LAG(sku_unit_cost) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id) AS prev_sku_unit_cost,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (ORDER BY unit_price DESC, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM with_details
    WINDOW w_sku_id_by_sku_unit_cost_ties AS (PARTITION BY sku_id ORDER BY sku_unit_cost), w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        moved_at,
        first_known_moved_at,
        sku_unit_cost_avg_3,
        rows_in_warehouse_id_move_type,
        unit_price_dense_rank,
        unit_price_cume_dist,
        last_unit_price,
        unit_price_percent_rank,
        prev_sku_unit_cost,
        NTH_VALUE(prev_sku_unit_cost, 2) OVER (PARTITION BY warehouse_id, unit_price_dense_rank_2 ORDER BY minutes_since_prev_moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_sku_unit_cost,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        ROUND(sku_unit_cost_dense_rank - AVG(sku_unit_cost_dense_rank) OVER (PARTITION BY warehouse_id), 2) AS sku_unit_cost_dense_rank_vs_warehouse_id_avg,
        MAX(unit_price_percent_rank) OVER w_warehouse_id AS warehouse_id_max_unit_price_percent_rank,
        ROUND(last_unit_price - AVG(last_unit_price) OVER w_warehouse_id, 2) AS last_unit_price_vs_warehouse_id_avg
    FROM windowed
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
)
SELECT
    id,
    sku_id,
    first_known_moved_at,
    rows_in_warehouse_id_move_type,
    unit_price_dense_rank,
    unit_price_cume_dist,
    last_unit_price,
    second_prev_sku_unit_cost,
    distinct_sku_id_in_warehouse_id,
    sku_unit_cost_dense_rank_vs_warehouse_id_avg,
    warehouse_id_max_unit_price_percent_rank,
    last_unit_price_vs_warehouse_id_avg,
    CASE WHEN unit_price_dense_rank >= 7 THEN 'HIGH' WHEN unit_price_dense_rank >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_dense_rank_tier
FROM sequenced
WHERE rows_in_warehouse_id_move_type >= 7
ORDER BY warehouse_id_max_unit_price_percent_rank DESC, unit_price_dense_rank_tier DESC NULLS FIRST
