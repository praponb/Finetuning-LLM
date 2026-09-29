WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj,
        CASE WHEN operator IN ('op_dew', 'op_ann') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj_2
    FROM stock_moves
    WHERE qty >= 67
      AND operator <> 'op_dew'
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        MIN(qty) AS min_move_qty,
        MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.min_move_qty,
        st.last_moved_at
    FROM filtered s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT
    sku_id,
    operator_group,
    COUNT(*) FILTER (WHERE move_type IS NOT NULL) AS n_move_type_is_not_null
FROM with_stats
GROUP BY ROLLUP(sku_id, operator_group)
ORDER BY sku_id NULLS LAST, operator_group NULLS LAST
