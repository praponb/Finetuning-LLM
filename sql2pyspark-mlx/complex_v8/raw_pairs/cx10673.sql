WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        ABS(unit_price - 206.8) AS unit_price_distance
    FROM stock_moves
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        unit_price - LAG(unit_price) OVER w_warehouse_id_by_unit_price_a AS unit_price_change,
        MAX(unit_price) OVER (PARTITION BY warehouse_id ORDER BY unit_price_adj DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_max_2_prior,
        ROW_NUMBER() OVER w_warehouse_id_by_unit_price_a AS unit_price_adj_seq,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY unit_price_adj DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type ORDER BY unit_price_adj NULLS LAST), 3) AS unit_price_adj_cume_dist,
        ROUND(CUME_DIST() OVER w_warehouse_id_by_unit_price_a_4, 3) AS unit_price_adj_cume_dist_2,
        ROUND(unit_price_distance - AVG(unit_price_distance) OVER (PARTITION BY warehouse_id), 2) AS unit_price_distance_vs_warehouse_id_avg,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), sku_id ORDER BY unit_price_distance, id) AS unit_price_distance_decile,
        MAX(unit_price_distance) OVER w_warehouse_id AS warehouse_id_max_unit_price_distance,
        MAX(unit_price) OVER w_warehouse_id AS warehouse_id_max_unit_price
    FROM recent_moves
    WINDOW w_warehouse_id_by_unit_price_a AS (PARTITION BY warehouse_id ORDER BY unit_price_adj DESC, id), w_warehouse_id_by_unit_price_a_4 AS (PARTITION BY warehouse_id ORDER BY unit_price_adj DESC), w_warehouse_id AS (PARTITION BY warehouse_id)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        moved_at,
        unit_price_adj,
        unit_price_adj_seq,
        unit_price_distance_vs_warehouse_id_avg,
        warehouse_id_max_unit_price_distance,
        warehouse_id_max_unit_price,
        ROUND(PERCENT_RANK() OVER w_unit_price_adj_seq_unit_pric, 3) AS unit_price_distance_vs_warehouse_id_avg_percent_,
        SUM(unit_price_adj_cume_dist) OVER (ORDER BY qty DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_price_adj_cume_dist_sum_1_prior,
        ROUND(CUME_DIST() OVER w_unit_price_adj_seq_unit_pric, 3) AS unit_price_distance_vs_warehouse_id_avg_cume_dis,
        NTILE(4) OVER (ORDER BY qty DESC, id) AS qty_quartile,
        SUM(qty) OVER (PARTITION BY unit_price_adj_seq, unit_price_distance_decile ORDER BY unit_price_distance_vs_warehouse_id_avg DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_sum_1_prior
    FROM ranked
    WINDOW w_unit_price_adj_seq_unit_pric AS (PARTITION BY unit_price_adj_seq, unit_price_distance_decile ORDER BY unit_price_distance_vs_warehouse_id_avg DESC, id)
)
SELECT
    id,
    moved_at,
    unit_price_adj_seq,
    unit_price_distance_vs_warehouse_id_avg,
    warehouse_id_max_unit_price_distance,
    warehouse_id_max_unit_price,
    unit_price_distance_vs_warehouse_id_avg_percent_,
    unit_price_distance_vs_warehouse_id_avg_cume_dis,
    qty_sum_1_prior,
    TO_DATE(moved_at) AS moved_at_date,
    DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
FROM ranked_2
ORDER BY unit_price_adj_seq DESC NULLS FIRST, warehouse_id_max_unit_price_distance DESC, id
LIMIT 5
