WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE unit_price <= 398.5
),
move_type_stats AS (
    -- Median qty per move_type
    SELECT move_type, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM base_moves
    GROUP BY move_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM base_moves s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.qty >= g.median_qty
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(median_qty, 1, 0) OVER (PARTITION BY sku_id ORDER BY median_qty DESC NULLS FIRST, id) AS next_median_qty,
        LEAD(unit_price) OVER (ORDER BY qty_minus_median DESC, id) AS next_unit_price,
        COUNT(*) OVER (PARTITION BY sku_id ORDER BY median_qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (ORDER BY qty_minus_median DESC) AS qty_minus_median_rank
    FROM vs_group
),
enriched AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.median_qty,
        s.qty_minus_median,
        s.running_count,
        s.qty_minus_median_rank,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM with_trends s
    JOIN warehouses w ON w.id = s.warehouse_id
),
derived AS (
    -- Add calculated columns
    SELECT *, moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d
    FROM enriched
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        median_qty,
        qty_minus_median,
        running_count,
        warehouse_opened_on,
        warehouse_city,
        moved_at_plus_7d
    FROM derived
    WHERE qty_minus_median_rank = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        operator,
        median_qty,
        qty_minus_median,
        running_count,
        warehouse_opened_on,
        moved_at_plus_7d,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_type)) AS distinct_operator_in_move_type,
        ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY qty_minus_median, id) AS seq_no,
        RANK() OVER (PARTITION BY operator, sku_id ORDER BY qty_minus_median) AS qty_minus_median_rank,
        ROW_NUMBER() OVER (ORDER BY median_qty DESC, id) AS median_qty_seq,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY operator, sku_id ORDER BY qty_minus_median, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM top_rows
)
-- Final projection
SELECT
    id,
    sku_id,
    qty_minus_median,
    warehouse_opened_on,
    moved_at_plus_7d,
    distinct_operator_in_move_type,
    seq_no,
    median_qty_seq,
    minutes_since_prev_moved_at,
    ABS(qty_minus_median - -50.0) AS qty_minus_median_distance,
    moved_at_plus_7d + INTERVAL 2 HOURS AS moved_at_plus_7d_plus_2h
FROM ranked
ORDER BY median_qty_seq DESC, distinct_operator_in_move_type
