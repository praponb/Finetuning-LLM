WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        CASE WHEN operator IN ('op_dew', 'op_ann') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE unit_price > 189.4
      AND HOUR(moved_at) BETWEEN 6 AND 8
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at
    FROM base s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY operator_group ORDER BY qty, id) AS rn,
        RANK() OVER (ORDER BY qty DESC) AS qty_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg
    FROM with_stats
)
SELECT
    rn,
    operator_group,
    MIN(qty_rank) AS min_qty_rank
FROM with_windows
GROUP BY ROLLUP(rn, operator_group)
ORDER BY rn NULLS LAST, operator_group NULLS LAST
