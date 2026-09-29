WITH valid AS (
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
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE sku_id <= 2
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND MONTH(st.moved_at) IN (1, 2)
        )
      AND s.operator IN ('op_ann', 'op_bee')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        moved_at_year,
        operator_prefix,
        DENSE_RANK() OVER (ORDER BY qty DESC, id) AS qty_dense_rank,
        ROUND(AVG(qty) OVER (ORDER BY qty ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at_year DESC, id), 3) AS moved_at_year_cume_dist,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY moved_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LAST_VALUE(moved_at) OVER (ORDER BY moved_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type
    FROM with_activity
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM with_windows
    WHERE qty_avg_4 > (SELECT MIN(qty_avg_4) FROM with_windows)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        moved_at_year,
        operator_prefix,
        qty_avg_4,
        moved_at_year_cume_dist,
        first_known_unit_price,
        last_moved_at,
        rows_in_move_type
    FROM filtered
    WHERE qty_dense_rank <= 5
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at_year,
    operator_prefix,
    qty_avg_4,
    moved_at_year_cume_dist,
    first_known_unit_price,
    last_moved_at,
    rows_in_move_type
FROM top_rows
ORDER BY operator_prefix, qty DESC
