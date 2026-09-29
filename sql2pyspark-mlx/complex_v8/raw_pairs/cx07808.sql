WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE unit_price >= 60.4
      AND warehouse_id >= 3
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, operator), 2) AS warehouse_id_operator_avg_unit_price,
        FIRST_VALUE(least_qty_unit_price) OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_least_qty_unit_price,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id, operator)) AS distinct_move_type_in_warehouse_id_operator,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY qty) AS qty_dense_rank
    FROM prepared_moves
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        least_qty_unit_price,
        warehouse_id_operator_avg_unit_price,
        first_least_qty_unit_price,
        distinct_move_type_in_warehouse_id_operator,
        least_qty_unit_price + qty_dense_rank AS least_qty_unit_price_plus_qty_dense_rank,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM with_trends
),
per_group AS (
    -- Summarise per group
    SELECT
        warehouse_id,
        YEAR(moved_at) AS moved_at_year,
        sku_id,
        ROUND(100 * COUNT(CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND', 'INBOUND') THEN 1 END) / COUNT(*), 1) AS pct_move_type_in_transfer_outbou,
        MAX(qty) AS max_qty
    FROM tagged
    GROUP BY 1, 2, 3
    HAVING SUM(least_qty_unit_price_plus_qty_dense_rank) > 179.2
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM per_group s
)
-- Final projection
SELECT
    warehouse_id,
    pct_move_type_in_transfer_outbou,
    max_qty,
    move_count,
    TO_DATE(first_moved_at) AS first_moved_at_date,
    first_moved_at + INTERVAL 2 HOURS AS first_moved_at_plus_2h
FROM with_stats
ORDER BY first_moved_at_plus_2h, max_qty DESC
