WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE (qty BETWEEN 109 AND 349 OR warehouse_id >= 5)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS unit_price_vs_warehouse_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at_dayofweek DESC), 3) AS moved_at_dayofweek_percent_rank,
        LEAD(qty, 3) OVER (ORDER BY moved_at, id) AS next3_qty,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at_dayofweek DESC) AS moved_at_dayofweek_dense_rank,
        LAG(unit_price, 2) OVER w_all_by_moved_at AS prev2_unit_price,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at, id), 3) AS moved_at_percent_rank,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_qty,
        ROW_NUMBER() OVER w_sku_id_operator_by_qty AS qty_seq,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_unit_price,
        MAX(unit_price) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        LEAD(moved_at_dayofweek, 1, 0) OVER (PARTITION BY sku_id ORDER BY moved_at_dayofweek DESC, id) AS next_moved_at_dayofweek,
        SUM(qty) OVER (PARTITION BY sku_id ORDER BY moved_at_dayofweek DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_sum_3_prior,
        LAG(moved_at) OVER (PARTITION BY sku_id, operator ORDER BY qty, id) AS prev_moved_at,
        unit_price - LAG(unit_price) OVER w_all_by_moved_at AS unit_price_change,
        ROUND(moved_at_dayofweek - AVG(moved_at_dayofweek) OVER (PARTITION BY warehouse_id), 2) AS moved_at_dayofweek_vs_warehouse_id_avg,
        ROUND(AVG(moved_at_dayofweek) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_moved_at_dayofweek
    FROM valid_moves
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at, id), w_sku_id_operator_by_qty AS (PARTITION BY sku_id, operator ORDER BY qty, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        moved_at_dayofweek_percent_rank,
        moved_at_dayofweek_dense_rank,
        warehouse_id_avg_qty,
        qty_seq,
        warehouse_id_avg_unit_price,
        max_unit_price_so_far,
        next_moved_at_dayofweek,
        qty_sum_3_prior,
        unit_price_change,
        moved_at_dayofweek_vs_warehouse_id_avg,
        LAST_VALUE(prev_moved_at) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY next3_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_moved_at,
        ROW_NUMBER() OVER (ORDER BY next3_qty DESC, id) AS next3_qty_seq,
        COUNT(*) OVER w_moved_at_to_date_moved_at_da AS rows_in_moved_at_to_date_moved_at_dayofweek,
        MAX(qty_sum_3_prior) OVER w_moved_at_to_date_moved_at_da AS moved_at_to_date_moved_at_dayofweek_max_qty_sum_,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY next3_qty DESC, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM windowed
    WINDOW w_moved_at_to_date_moved_at_da AS (PARTITION BY TO_DATE(moved_at), moved_at_dayofweek)
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at_dayofweek_percent_rank,
    moved_at_dayofweek_dense_rank,
    warehouse_id_avg_qty,
    qty_seq,
    warehouse_id_avg_unit_price,
    max_unit_price_so_far,
    qty_sum_3_prior,
    unit_price_change,
    moved_at_dayofweek_vs_warehouse_id_avg,
    last_known_prev_moved_at,
    next3_qty_seq,
    rows_in_moved_at_to_date_moved_at_dayofweek,
    minutes_since_prev_moved_at,
    LENGTH(TRIM(operator)) AS operator_len,
    YEAR(last_known_prev_moved_at) AS last_known_prev_moved_at_year
FROM with_windows
ORDER BY operator_len NULLS LAST
