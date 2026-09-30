WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN move_type IN ('OUTBOUND', 'INBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE (unit_price < 372.6 OR HOUR(moved_at) BETWEEN 6 AND 9)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at)), 2) AS unit_price_vs_sku_id_moved_at_month_avg,
        MIN(unit_price) OVER (ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far
    FROM recent_moves
)
-- Subtotals with ROLLUP
SELECT
    sku_id,
    move_type_group,
    GROUPING_ID() AS gid,
    ROUND(100 * COUNT(CASE WHEN operator = 'op_eve' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_eve,
    MAX(moved_at) AS last_moved_at,
    MIN(moved_at) AS first_moved_at
FROM with_windows
GROUP BY ROLLUP(sku_id, move_type_group)
