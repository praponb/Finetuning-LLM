WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE (unit_price BETWEEN 119.5 AND 372.6 OR sku_id IS NOT NULL)
),
scored AS (
    -- Label and score rows
    SELECT id, qty, UPPER(TRIM(operator)) AS operator_clean
    FROM scoped
    WHERE HOUR(moved_at) BETWEEN 10 AND 13
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY qty ASC), 3) AS qty_cume_dist,
        NTH_VALUE(qty, 2) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM scored
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_all_by_qty AS qty_decile,
        NTH_VALUE(qty_cume_dist, 2) OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_cume_dist
    FROM ranked
    WINDOW w_all_by_qty AS (ORDER BY qty NULLS LAST, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY qty_decile) AS rows_in_qty_decile,
        ROW_NUMBER() OVER w_qty_decile_by_second_qty AS second_qty_seq
    FROM with_windows
    WINDOW w_qty_decile_by_second_qty AS (PARTITION BY qty_decile ORDER BY second_qty ASC, id)
),
scored_2 AS (
    -- Label and score rows
    SELECT
        id,
        qty,
        qty_cume_dist,
        second_qty,
        qty_decile,
        rows_in_qty_decile,
        ROUND(second_qty / NULLIF(rows_in_qty_decile, 0), 3) AS second_qty_per_rows_in_qty_decile,
        CASE WHEN second_qty_cume_dist >= 0.7 THEN 'large' WHEN second_qty_cume_dist >= 0.2 THEN 'medium' ELSE 'small' END AS second_qty_cume_dist_tier
    FROM with_trends
    WHERE qty < 426
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, rows_in_qty_decile, qty_decile, qty, second_qty
    FROM scored_2
) src
UNPIVOT (
    reading FOR metric IN (rows_in_qty_decile, qty_decile, qty, second_qty)
) unp
