WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        TO_DATE(moved_at) AS moved_at_date,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE warehouse_id >= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(unit_price_filled / NULLIF(SUM(unit_price_filled) OVER (PARTITION BY operator, sku_id), 0), 4) AS unit_price_filled_share_of_operator_sku_id,
        DATEDIFF(moved_at_date, LAG(moved_at_date) OVER w_moved_at_month_warehouse_id) AS days_since_prev_moved_at_date,
        LAST_VALUE(unit_price_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price_filled,
        ROUND(AVG(unit_price_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY qty DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_filled_avg_3_prior,
        unit_price_filled - LAG(unit_price_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY qty DESC, id) AS unit_price_filled_change
    FROM scoped_moves
    WINDOW w_moved_at_month_warehouse_id AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY qty DESC, id)
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM windowed s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
-- Subtotals with GROUPING SETS
SELECT
    move_count,
    operator,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN operator IN ('op_ann', 'op_dew') THEN 1 ELSE 0 END) AS n_operator_in_op_ann_op_dew,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
    ROUND(STDDEV(last_unit_price_filled), 2) AS stddev_last_unit_price_filled
FROM with_stats
GROUP BY GROUPING SETS ((move_count, operator), (move_count), ())
