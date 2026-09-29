WITH recent_moves AS (
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
        CASE WHEN unit_price >= 289.7 THEN 'HIGH' WHEN unit_price >= 209.4 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE move_type IN ('TRANSFER', 'INBOUND')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id), 0), 4) AS qty_share_of_sku_id,
        MAX(qty) OVER (PARTITION BY sku_id) AS sku_id_max_qty,
        RANK() OVER (PARTITION BY operator ORDER BY qty_per_unit_price) AS qty_per_unit_price_rank,
        SIZE(COLLECT_SET(move_type) OVER w_sku_id) AS distinct_move_type_in_sku_id,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, unit_price_tier ORDER BY qty_per_unit_price DESC, id) AS rn,
        SUM(qty) OVER (PARTITION BY warehouse_id, unit_price_tier ORDER BY qty_per_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        ROUND(qty_per_unit_price - AVG(qty_per_unit_price) OVER (PARTITION BY warehouse_id), 2) AS qty_per_unit_price_vs_warehouse_id_avg,
        ROUND(AVG(qty) OVER w_sku_id, 2) AS sku_id_avg_qty,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id), 0), 4) AS unit_price_share_of_sku_id,
        ROUND(PERCENT_RANK() OVER w_move_type_by_moved_at_ties, 3) AS moved_at_percent_rank,
        DENSE_RANK() OVER w_move_type_by_moved_at_ties AS moved_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY operator ORDER BY qty_per_unit_price, id), 3) AS qty_per_unit_price_cume_dist
    FROM recent_moves
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_move_type_by_moved_at_ties AS (PARTITION BY move_type ORDER BY moved_at DESC)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        qty_per_unit_price - LAG(qty_per_unit_price) OVER (PARTITION BY warehouse_id ORDER BY qty_per_unit_price_rank DESC, id) AS qty_per_unit_price_change,
        DENSE_RANK() OVER (PARTITION BY distinct_move_type_in_sku_id, warehouse_id ORDER BY sku_id_avg_qty DESC) AS sku_id_avg_qty_dense_rank,
        LAG(moved_at) OVER w_warehouse_id_by_qty_per_unit AS prev_moved_at,
        LAST_VALUE(unit_price) OVER (PARTITION BY distinct_move_type_in_sku_id, warehouse_id ORDER BY sku_id_avg_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
    FROM with_windows
    WINDOW w_warehouse_id_by_qty_per_unit AS (PARTITION BY warehouse_id ORDER BY qty_per_unit_price_rank DESC, id)
),
stats AS (
    -- Summarise per group
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        MIN(qty) AS min_qty,
        ROUND(100 * COUNT(CASE WHEN move_type = 'ADJUST' THEN 1 END) / COUNT(*), 1) AS pct_move_type_adjust,
        MAX(moved_at) AS last_moved_at,
        ROUND(SUM(unit_price) / NULLIF(SUM(sku_id_avg_qty_dense_rank), 0), 3) AS unit_price_to_sku_id_avg_qty_dense_rank_ratio,
        ROUND(AVG(unit_price_share_of_sku_id), 2) AS avg_unit_price_share_of_sku_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM sequenced
    GROUP BY 1
)
-- Final projection
SELECT
    moved_at_day,
    min_qty,
    pct_move_type_adjust,
    last_moved_at,
    avg_unit_price_share_of_sku_id,
    operator_list,
    last_moved_at + INTERVAL 7 DAYS AS last_moved_at_plus_7d,
    COALESCE(min_qty, 0) AS min_qty_filled
FROM stats
ORDER BY avg_unit_price_share_of_sku_id NULLS LAST, last_moved_at DESC NULLS FIRST
