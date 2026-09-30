WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM valid s
    JOIN skus sk ON sk.id = s.sku_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        sku_code,
        sku_unit_cost,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER w_warehouse_id, 2) AS sku_unit_cost_vs_warehouse_id_avg,
        ROUND(AVG(qty) OVER (ORDER BY moved_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type, warehouse_id ORDER BY qty ASC), 3) AS qty_cume_dist,
        ROUND(AVG(qty) OVER (ORDER BY moved_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior,
        SUM(qty) OVER (PARTITION BY warehouse_id, move_type) AS warehouse_id_move_type_sum_qty,
        SUM(qty) OVER (ORDER BY moved_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_sum_2,
        FIRST_VALUE(qty) OVER (PARTITION BY move_type, warehouse_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        MAX(qty) OVER (PARTITION BY move_type, warehouse_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        MIN(sku_unit_cost) OVER (PARTITION BY warehouse_id, move_type) AS warehouse_id_move_type_min_sku_unit_cost,
        DENSE_RANK() OVER w_all_by_moved_at_ties AS moved_at_dense_rank,
        RANK() OVER (ORDER BY qty ASC, id) AS qty_rank,
        RANK() OVER w_move_type_warehouse_id_by_qt AS qty_rank_2
    FROM with_details_sku
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_all_by_moved_at_ties AS (ORDER BY moved_at), w_move_type_warehouse_id_by_qt AS (PARTITION BY move_type, warehouse_id ORDER BY qty ASC)
)
SELECT
    id,
    warehouse_id,
    sku_unit_cost_vs_warehouse_id_avg,
    qty_avg_4,
    qty_cume_dist,
    qty_avg_1_prior,
    warehouse_id_move_type_sum_qty,
    qty_sum_2,
    max_qty_so_far,
    last_known_moved_at,
    moved_at_dense_rank,
    qty_rank,
    qty_rank_2,
    LOWER(REGEXP_REPLACE(TRIM(sku_code), ' ', '_')) AS sku_code_slug
FROM ranked
