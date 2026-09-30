WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE unit_price < 396.4
      AND warehouse_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        moved_at_dayofweek,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY moved_at_dayofweek ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        SUM(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS qty_sum_4,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id), 2) AS qty_vs_sku_id_avg,
        LEAD(qty) OVER (PARTITION BY moved_at_dayofweek, warehouse_id ORDER BY qty, id) AS next_qty,
        DENSE_RANK() OVER w_moved_at_dayofweek_by_moved_2 AS moved_at_dense_rank,
        SUM(unit_price) OVER (PARTITION BY moved_at_dayofweek ORDER BY moved_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_sum_2,
        DENSE_RANK() OVER (ORDER BY qty DESC) AS qty_dense_rank,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        ROUND(CUME_DIST() OVER w_moved_at_dayofweek_by_moved_2, 3) AS moved_at_cume_dist,
        FIRST_VALUE(moved_at_dayofweek) IGNORE NULLS OVER (PARTITION BY moved_at_dayofweek ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at_dayofweek,
        MAX(moved_at_dayofweek) OVER (PARTITION BY sku_id) AS sku_id_max_moved_at_dayofweek
    FROM filtered
    WINDOW w_moved_at_dayofweek_by_moved_2 AS (PARTITION BY moved_at_dayofweek ORDER BY moved_at ASC, id), w_sku_id AS (PARTITION BY sku_id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        unit_price,
        moved_at_dayofweek,
        last_known_unit_price,
        qty_sum_4,
        moved_at_dense_rank,
        rows_in_sku_id,
        moved_at_cume_dist,
        sku_id_max_moved_at_dayofweek,
        COUNT(*) OVER w_first_known_moved_at_dayofwe AS rows_in_first_known_moved_at_dayofweek_moved_at_,
        ROUND(qty_vs_sku_id_avg / NULLIF(SUM(qty_vs_sku_id_avg) OVER (PARTITION BY first_known_moved_at_dayofweek, moved_at_dense_rank), 0), 4) AS qty_vs_sku_id_avg_share_of_first_known_moved_at_,
        ROUND(unit_price_sum_2 / NULLIF(SUM(unit_price_sum_2) OVER w_first_known_moved_at_dayofwe, 0), 4) AS unit_price_sum_2_share_of_first_known_moved_at_d
    FROM ranked
    WINDOW w_first_known_moved_at_dayofwe AS (PARTITION BY first_known_moved_at_dayofweek, moved_at_dense_rank)
)
-- Final projection
SELECT
    id,
    moved_at_dayofweek,
    last_known_unit_price,
    moved_at_dense_rank,
    rows_in_sku_id,
    moved_at_cume_dist,
    sku_id_max_moved_at_dayofweek,
    rows_in_first_known_moved_at_dayofweek_moved_at_,
    qty_vs_sku_id_avg_share_of_first_known_moved_at_,
    unit_price_sum_2_share_of_first_known_moved_at_d,
    ABS(last_known_unit_price - 264.8) AS last_known_unit_price_distance,
    ABS(rows_in_sku_id - 12) AS rows_in_sku_id_distance
FROM with_trends
ORDER BY rows_in_sku_id_distance
