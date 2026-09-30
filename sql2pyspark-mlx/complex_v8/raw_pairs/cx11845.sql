WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        LOWER(TRIM(move_type)) AS move_type_clean,
        CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref,
        CASE WHEN operator IN ('op_eve', 'op_dew') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND TO_DATE(moved_at) < DATE '2025-02-06'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(qty) OVER (ORDER BY qty ASC, id) AS next_qty,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_all_by_qty)) / 60, 1) AS minutes_since_prev_moved_at,
        LAG(qty) OVER (ORDER BY qty ASC, id) AS prev_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty ASC), 3) AS qty_cume_dist
    FROM cleaned_moves
    WINDOW w_all_by_qty AS (ORDER BY qty ASC, id)
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, SUM(unit_price) AS total_move_unit_price, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.total_move_unit_price,
        COALESCE(st.move_count, 0) AS move_count
    FROM with_windows s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, total_move_unit_price, qty_cume_dist
    FROM with_stats
) src
UNPIVOT (
    reading FOR metric IN (total_move_unit_price, qty_cume_dist)
) unp
ORDER BY id, metric
