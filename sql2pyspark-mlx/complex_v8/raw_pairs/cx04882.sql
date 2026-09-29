WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CASE WHEN unit_price >= 363.6 THEN 'large' WHEN unit_price >= 130.6 THEN 'medium' ELSE 'small' END AS unit_price_tier,
        ABS(qty - 244) AS qty_distance
    FROM stock_moves
),
joined AS (
    -- Join skus
    SELECT s.*, sk.is_active AS sku_is_active, sk.sku_code
    FROM filtered_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        qty_distance,
        sku_code,
        ROUND(AVG(qty) OVER w_sku_id, 2) AS sku_id_avg_qty,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty_distance DESC, id) AS rn,
        DENSE_RANK() OVER (ORDER BY qty_distance) AS qty_distance_dense_rank,
        NTILE(5) OVER w_sku_is_active_by_moved_at AS moved_at_quintile,
        LEAD(qty, 1, 0) OVER w_all_by_qty_distance AS next_qty,
        SIZE(COLLECT_SET(unit_price_tier) OVER (PARTITION BY YEAR(moved_at), warehouse_id)) AS distinct_unit_price_tier_in_moved_at_year_wareho,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY qty_distance DESC) AS qty_distance_dense_rank_2,
        ROUND(AVG(qty_distance) OVER (ORDER BY qty_distance, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_distance_avg_2,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY YEAR(moved_at), warehouse_id), 0), 4) AS qty_share_of_moved_at_year_warehouse_id,
        SUM(qty) OVER (PARTITION BY sku_id) AS sku_id_sum_qty,
        NTH_VALUE(qty_distance, 2) OVER (PARTITION BY warehouse_id, operator ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_distance
    FROM joined
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_sku_is_active_by_moved_at AS (PARTITION BY sku_is_active ORDER BY moved_at NULLS LAST, id), w_all_by_qty_distance AS (ORDER BY qty_distance, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(next_qty) OVER (PARTITION BY qty_distance_dense_rank_2 ORDER BY qty_distance_dense_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY rn), 0), 4) AS qty_share_of_rn,
        MIN(qty_distance) OVER (PARTITION BY qty_distance_dense_rank_2 ORDER BY qty_distance_dense_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_distance_so_far,
        ROUND(sku_id_avg_qty / NULLIF(SUM(sku_id_avg_qty) OVER (PARTITION BY rn), 0), 4) AS sku_id_avg_qty_share_of_rn,
        RANK() OVER (ORDER BY next_qty ASC, id) AS next_qty_rank,
        NTH_VALUE(moved_at_quintile, 2) OVER (PARTITION BY qty_distance_dense_rank_2 ORDER BY qty_distance_dense_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_quintile,
        ROUND(AVG(qty_distance) OVER (PARTITION BY rn), 2) AS rn_avg_qty_distance,
        MAX(moved_at_quintile) OVER (PARTITION BY rn) AS rn_max_moved_at_quintile
    FROM windowed
)
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator,
    qty_distance,
    sku_code,
    sku_id_avg_qty,
    rn,
    moved_at_quintile,
    next_qty,
    distinct_unit_price_tier_in_moved_at_year_wareho,
    qty_distance_dense_rank_2,
    qty_distance_avg_2,
    qty_share_of_moved_at_year_warehouse_id,
    second_qty_distance,
    last_next_qty,
    qty_share_of_rn,
    min_qty_distance_so_far,
    sku_id_avg_qty_share_of_rn,
    next_qty_rank,
    second_moved_at_quintile,
    rn_avg_qty_distance,
    rn_max_moved_at_quintile
FROM sequenced
ORDER BY rn
