WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        HOUR(moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE unit_price < 323.9
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        MAX(unit_price) OVER w_sku_id AS sku_id_max_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg,
        ROUND(AVG(moved_at_hour) OVER (ORDER BY moved_at_hour ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moved_at_hour_avg_7,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        RANK() OVER w_all_by_moved_at_hour_ties AS moved_at_hour_rank,
        ROUND(AVG(moved_at_hour) OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS moved_at_hour_avg_3_prior,
        ROUND(AVG(moved_at_hour) OVER (PARTITION BY sku_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS moved_at_hour_avg_2,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id, sku_id)) AS distinct_move_type_in_warehouse_id_sku_id,
        SIZE(COLLECT_SET(move_type) OVER w_sku_id) AS distinct_move_type_in_sku_id,
        ROW_NUMBER() OVER (ORDER BY moved_at_hour ASC, id) AS moved_at_hour_seq,
        LAG(moved_at) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at ASC, id) AS prev_moved_at
    FROM filtered_moves
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_all_by_moved_at_hour_ties AS (ORDER BY moved_at_hour ASC)
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_distinct_warehouse_id_in_sku, 3) AS unit_price_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_warehouse_id_in_sku_id, warehouse_id ORDER BY unit_price DESC, id), 3) AS unit_price_cume_dist,
        ROUND(moved_at_hour_avg_3_prior / NULLIF(SUM(moved_at_hour_avg_3_prior) OVER (PARTITION BY warehouse_id), 0), 4) AS moved_at_hour_avg_3_prior_share_of_warehouse_id
    FROM with_windows
    WINDOW w_distinct_warehouse_id_in_sku AS (PARTITION BY distinct_warehouse_id_in_sku_id, warehouse_id ORDER BY unit_price DESC)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    unit_price,
    moved_at_hour,
    distinct_warehouse_id_in_sku_id,
    rows_in_sku_id,
    sku_id_max_unit_price,
    moved_at_hour_avg_7,
    moved_at_hour_rank,
    moved_at_hour_avg_3_prior,
    moved_at_hour_avg_2,
    distinct_move_type_in_warehouse_id_sku_id,
    distinct_move_type_in_sku_id,
    moved_at_hour_seq,
    unit_price_percent_rank,
    unit_price_cume_dist,
    moved_at_hour_avg_3_prior_share_of_warehouse_id,
    LENGTH(TRIM(move_type)) AS move_type_len,
    TO_DATE(second_moved_at) AS second_moved_at_date
FROM with_windows_2
ORDER BY second_moved_at_date DESC, moved_at_hour_seq DESC
