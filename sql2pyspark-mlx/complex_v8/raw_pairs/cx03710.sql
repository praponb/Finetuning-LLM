WITH base_moves AS (
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
        qty + unit_price AS qty_plus_unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
    WHERE qty BETWEEN 45 AND 357
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_plus_unit_price,
        LOWER(TRIM(moved_at_label)) AS moved_at_label_clean,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM base_moves
    WHERE warehouse_id <= 3
),
move_type_or_unknown_stats AS (
    -- Median qty per move_type_or_unknown
    SELECT move_type_or_unknown, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM derived
    GROUP BY move_type_or_unknown
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM derived s
    JOIN move_type_or_unknown_stats g ON g.move_type_or_unknown = s.move_type_or_unknown
    WHERE s.qty >= g.median_qty
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        moved_at,
        qty_plus_unit_price,
        move_type_or_unknown,
        median_qty,
        qty_minus_median,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM compared
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        moved_at,
        qty_plus_unit_price,
        days_since_moved_at,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        MONTH(moved_at) AS moved_at_month
    FROM annotated
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        moved_at,
        qty_plus_unit_price,
        days_since_moved_at,
        moved_at_label,
        moved_at_month,
        ROUND(AVG(moved_at_month) OVER (ORDER BY qty_plus_unit_price ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS moved_at_month_avg_6_prior,
        ROUND(CUME_DIST() OVER (ORDER BY qty_plus_unit_price ASC), 3) AS qty_plus_unit_price_cume_dist,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (ORDER BY moved_at_month DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at_month DESC NULLS FIRST, id), 3) AS moved_at_month_percent_rank,
        MAX(days_since_moved_at) OVER (ORDER BY qty_plus_unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_moved_at_so_far
    FROM tagged
)
SELECT
    id,
    moved_at,
    qty_plus_unit_price,
    moved_at_label,
    moved_at_month,
    moved_at_month_avg_6_prior,
    qty_plus_unit_price_cume_dist,
    max_days_since_moved_at_so_far,
    moved_at_month + qty_plus_unit_price AS moved_at_month_plus_qty_plus_unit_price
FROM with_trends
ORDER BY qty_plus_unit_price_cume_dist DESC, moved_at_month_plus_qty_plus_unit_price DESC
