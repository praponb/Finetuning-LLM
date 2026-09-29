WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        ABS(unit_price - 264.8) AS unit_price_distance
    FROM stock_moves
    WHERE unit_price >= 215.4
      AND operator = 'op_bee'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price_distance,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        MAX(unit_price_distance) OVER (PARTITION BY warehouse_id, move_type) AS warehouse_id_move_type_max_unit_price_distance,
        ROUND(unit_price_distance - AVG(unit_price_distance) OVER (PARTITION BY sku_id), 2) AS unit_price_distance_vs_sku_id_avg,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id, move_type), 2) AS qty_vs_warehouse_id_move_type_avg,
        ROW_NUMBER() OVER (ORDER BY unit_price_distance, id) AS rn,
        MAX(unit_price_distance) OVER (ORDER BY qty DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_distance_max_6_prior,
        ROUND(CUME_DIST() OVER w_sku_id_by_unit_price_distanc, 3) AS unit_price_distance_cume_dist,
        LAST_VALUE(moved_at) OVER (PARTITION BY sku_id ORDER BY unit_price_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at
    FROM scoped_moves
    WINDOW w_sku_id_by_unit_price_distanc AS (PARTITION BY sku_id ORDER BY unit_price_distance)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(sku_id_avg_qty) OVER (ORDER BY warehouse_id_move_type_max_unit_price_distance DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS sku_id_avg_qty_avg_6_prior,
        MIN(rn) OVER (PARTITION BY sku_id) AS sku_id_min_rn,
        MAX(unit_price_distance) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price_distance,
        NTILE(3) OVER (ORDER BY warehouse_id_move_type_max_unit_price_distance DESC, id) AS warehouse_id_move_type_max_unit_price_distance_t,
        NTILE(5) OVER w_rn_warehouse_id_by_last_move AS last_moved_at_quintile
    FROM windowed
    WINDOW w_rn_warehouse_id_by_last_move AS (PARTITION BY rn, warehouse_id ORDER BY last_moved_at ASC, id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    unit_price_distance,
    sku_id_avg_qty,
    warehouse_id_move_type_max_unit_price_distance,
    unit_price_distance_vs_sku_id_avg,
    second_moved_at,
    unit_price_distance_cume_dist,
    last_moved_at,
    sku_id_avg_qty_avg_6_prior,
    sku_id_min_rn,
    sku_id_max_unit_price_distance,
    warehouse_id_move_type_max_unit_price_distance_t,
    last_moved_at_quintile,
    CONCAT(move_type, '#', CAST(last_moved_at_quintile AS STRING)) AS move_type_ref
FROM sequenced
WHERE unit_price_distance < 275.0
ORDER BY sku_id NULLS LAST
