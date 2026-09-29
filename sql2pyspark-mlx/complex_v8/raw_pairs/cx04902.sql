WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        CASE WHEN operator IN ('op_chan', 'op_eve') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE move_type IN ('INBOUND', 'OUTBOUND', 'ADJUST')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator_group,
        operator_or_unknown,
        ROUND(CUME_DIST() OVER (PARTITION BY operator_or_unknown, warehouse_id ORDER BY moved_at, id), 3) AS moved_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY operator_or_unknown, warehouse_id ORDER BY moved_at, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM valid_moves
)
SELECT
    move_type,
    sku_id,
    PERCENTILE_APPROX(minutes_since_prev_moved_at, 0.5) AS median_minutes_since_prev_moved_at,
    PERCENTILE_APPROX(moved_at_cume_dist, 0.5) AS median_moved_at_cume_dist,
    ROUND(100 * COUNT(CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 1 END) / COUNT(*), 1) AS pct_move_type_in_adjust_inbound
FROM ranked
GROUP BY GROUPING SETS ((move_type, sku_id), (move_type), ())
