WITH recent AS (
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
        LEAST(qty, unit_price) AS least_qty_unit_price,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj
    FROM stock_moves
    WHERE operator <> 'op_ann'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        YEAR(moved_at) AS moved_at_year,
        MIN(moved_at) AS first_moved_at,
        ROUND(STDDEV(least_qty_unit_price), 2) AS stddev_least_qty_unit_price,
        ROUND(AVG(least_qty_unit_price), 2) AS avg_least_qty_unit_price,
        COUNT(*) AS cnt,
        SUM(unit_price) AS total_unit_price
    FROM recent
    GROUP BY move_type, YEAR(moved_at)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY avg_least_qty_unit_price ASC, move_type, moved_at_year) AS seq_no,
        COUNT(*) OVER w_cnt_first_moved_at_month AS rows_in_cnt_first_moved_at_month,
        ROUND(moved_at_year / NULLIF(SUM(moved_at_year) OVER (PARTITION BY cnt, DATE_TRUNC('MONTH', first_moved_at)), 0), 4) AS moved_at_year_share_of_cnt_first_moved_at_month
    FROM summary
    WINDOW w_cnt_first_moved_at_month AS (PARTITION BY cnt, DATE_TRUNC('MONTH', first_moved_at))
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(cnt, moved_at_year) AS greatest_cnt_moved_at_year,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM ranked
    WHERE avg_least_qty_unit_price BETWEEN 100.8 AND 325.4
)
-- Native SQL UNPIVOT (wide to long)
SELECT move_type, moved_at_year, metric_name, value
FROM (
    SELECT move_type, moved_at_year, rows_in_cnt_first_moved_at_month, cnt
    FROM scored
) src
UNPIVOT (
    value FOR metric_name IN (rows_in_cnt_first_moved_at_month, cnt)
) unp
ORDER BY move_type, moved_at_year, metric_name
