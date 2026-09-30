WITH base AS (
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
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM stock_moves
    WHERE warehouse_id <= 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type) AS distinct_sku_id_in_move_type,
        LEAD(qty) OVER (PARTITION BY MONTH(moved_at_year) ORDER BY unit_price DESC, id) AS next_qty,
        DENSE_RANK() OVER (ORDER BY moved_at_year, id) AS moved_at_year_dense_rank,
        COUNT(*) OVER w_move_type AS rows_in_move_type,
        RANK() OVER (PARTITION BY MONTH(moved_at_year) ORDER BY unit_price DESC, id) AS unit_price_rank,
        SUM(qty) OVER (ORDER BY moved_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM base
    WINDOW w_move_type AS (PARTITION BY move_type)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE next_qty < (SELECT MAX(next_qty) FROM with_trends)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    moved_at_year,
    distinct_sku_id_in_move_type,
    next_qty,
    rows_in_move_type,
    running_qty,
    DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
FROM with_activity
ORDER BY sku_id
