WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
      AND warehouse_id IS NOT NULL
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
      AND s.days_since_moved_at <= 560
),
warehouse_id_stats AS (
    -- Median qty per warehouse_id
    SELECT warehouse_id, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM filtered
    GROUP BY warehouse_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM filtered s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.qty >= g.median_qty
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty_per_unit_price,
        s.qty_minus_median,
        st.id AS move_id,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id,
        st.move_type AS move_move_type
    FROM outliers s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
stats AS (
    -- Summarise per group
    SELECT
        warehouse_id,
        move_id,
        ARRAY_SORT(COLLECT_SET(move_operator)) AS move_operator_list,
        ROUND(SUM(qty_minus_median) / NULLIF(SUM(qty_per_unit_price), 0), 3) AS qty_minus_median_to_qty_per_unit_price_ratio,
        MIN(qty_minus_median) AS min_qty_minus_median,
        COUNT(*) AS n_rows
    FROM line_level
    GROUP BY 1, 2
    HAVING SUM(qty_minus_median) > -250.0
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE n_rows > (SELECT AVG(n_rows) FROM stats)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_min_qty_minus_median, 3) AS min_qty_minus_median_percent_rank,
        COUNT(*) OVER (PARTITION BY move_id) AS rows_in_move_id,
        LAG(n_rows, 3) OVER (ORDER BY min_qty_minus_median DESC, warehouse_id, move_id) AS prev3_n_rows,
        LEAD(n_rows, 2, 0) OVER (PARTITION BY move_id, warehouse_id ORDER BY qty_minus_median_to_qty_per_unit_price_ratio, warehouse_id, move_id) AS next2_n_rows
    FROM qualified
    WINDOW w_all_by_min_qty_minus_median AS (ORDER BY min_qty_minus_median DESC)
)
SELECT warehouse_id, move_id, metric_name, metric_value
FROM (
    SELECT warehouse_id, move_id, min_qty_minus_median, qty_minus_median_to_qty_per_unit_price_ratio
    FROM sequenced
) src
UNPIVOT (
    metric_value FOR metric_name IN (min_qty_minus_median, qty_minus_median_to_qty_per_unit_price_ratio)
) unp
ORDER BY warehouse_id, move_id, metric_name
