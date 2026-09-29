WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price - qty AS unit_price_minus_qty,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM stock_moves
    WHERE move_type IN ('OUTBOUND', 'TRANSFER', 'ADJUST')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price_minus_qty,
        moved_at_label,
        DENSE_RANK() OVER (PARTITION BY move_type, MONTH(moved_at) ORDER BY unit_price NULLS LAST) AS unit_price_dense_rank,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY YEAR(moved_at))) AS distinct_move_type_in_moved_at_year,
        SUM(qty) OVER w_moved_at_year AS moved_at_year_sum_qty,
        ROUND(AVG(qty) OVER (PARTITION BY YEAR(moved_at)), 2) AS moved_at_year_avg_qty,
        SUM(unit_price_minus_qty) OVER (PARTITION BY move_type, MONTH(moved_at) ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_minus_qty,
        RANK() OVER w_all_by_qty_ties AS qty_rank,
        COUNT(*) OVER (PARTITION BY YEAR(moved_at)) AS rows_in_moved_at_year,
        DENSE_RANK() OVER (PARTITION BY sku_id, YEAR(moved_at) ORDER BY qty DESC, id) AS qty_dense_rank,
        NTILE(10) OVER (PARTITION BY sku_id, YEAR(moved_at) ORDER BY qty DESC, id) AS qty_decile
    FROM prepared
    WINDOW w_moved_at_year AS (PARTITION BY YEAR(moved_at)), w_all_by_qty_ties AS (ORDER BY qty DESC)
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(qty) OVER (PARTITION BY qty_rank, sku_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        SUM(unit_price) OVER (PARTITION BY qty_decile ORDER BY unit_price_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        COUNT(*) OVER w_move_type AS rows_in_move_type,
        ROUND(AVG(moved_at_year_avg_qty) OVER w_move_type, 2) AS move_type_avg_moved_at_year_avg_qty
    FROM with_trends
    WINDOW w_move_type AS (PARTITION BY move_type)
)
SELECT
    id,
    move_type,
    qty,
    unit_price,
    unit_price_minus_qty,
    moved_at_label,
    unit_price_dense_rank,
    moved_at_year_sum_qty,
    moved_at_year_avg_qty,
    running_unit_price_minus_qty,
    rows_in_moved_at_year,
    qty_dense_rank,
    qty_decile,
    last_qty,
    running_unit_price,
    rows_in_move_type,
    LEAST(unit_price_dense_rank, moved_at_year_sum_qty) AS least_unit_price_dense_rank_moved_at_year_sum_qt,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
FROM with_trends_2
ORDER BY moved_at_year_avg_qty
