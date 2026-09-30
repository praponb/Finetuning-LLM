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
        CASE WHEN unit_price >= 361.9 THEN 'HIGH' WHEN unit_price >= 125.9 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM stock_moves
    WHERE operator NOT IN ('op_eve', 'op_dew')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_tier,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator ORDER BY moved_at), 3) AS moved_at_percent_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type), 2) AS unit_price_vs_move_type_avg,
        MAX(unit_price) OVER w_moved_at_month_warehouse_id AS moved_at_month_warehouse_id_max_unit_price,
        SUM(qty) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS qty_sum_3,
        COUNT(*) OVER (ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(unit_price) OVER (PARTITION BY move_type) AS move_type_max_unit_price,
        LEAD(qty) OVER (PARTITION BY move_type, unit_price_tier ORDER BY qty ASC NULLS LAST, id) AS next_qty,
        MIN(qty) OVER (PARTITION BY move_type, unit_price_tier ORDER BY qty ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY moved_at, id) AS moved_at_seq
    FROM valid_moves
    WINDOW w_moved_at_month_warehouse_id AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    qty,
    moved_at,
    operator,
    unit_price,
    unit_price_tier,
    moved_at_percent_rank,
    unit_price_vs_move_type_avg,
    moved_at_month_warehouse_id_max_unit_price,
    qty_sum_3,
    move_type_max_unit_price,
    next_qty,
    min_qty_so_far,
    moved_at_seq
FROM metrics
ORDER BY moved_at DESC, moved_at_seq DESC
