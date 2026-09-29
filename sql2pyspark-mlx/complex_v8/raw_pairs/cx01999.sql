WITH base AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_TRUNC('YEAR', opened_on) AS opened_on_year,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        CONCAT_WS(' / ', city, wh_code) AS city_wh_code_label
    FROM warehouses
    WHERE capacity <= 11005
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.opened_on,
        s.opened_on_year,
        s.capacity_bucket,
        s.city_wh_code_label,
        st.id AS move_id,
        st.qty AS move_qty,
        st.move_type,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator
    FROM base s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id
),
move_operator_stats AS (
    -- Median move_qty per move_operator
    SELECT move_operator, PERCENTILE_APPROX(move_qty, 0.5) AS median_move_qty
    FROM with_children_moves
    GROUP BY move_operator
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_move_qty,
        ROUND(s.move_qty - g.median_move_qty, 2) AS move_qty_minus_median
    FROM with_children_moves s
    JOIN move_operator_stats g ON g.move_operator = s.move_operator
    WHERE s.move_qty >= g.median_move_qty
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_on_year,
        capacity_bucket,
        city_wh_code_label,
        move_id,
        move_type,
        move_moved_at,
        median_move_qty,
        move_qty_minus_median,
        DENSE_RANK() OVER w_move_operator_move_type_by_m AS move_qty_minus_median_dense_rank,
        ROUND((UNIX_TIMESTAMP(opened_on_year) - UNIX_TIMESTAMP(LAG(opened_on_year) OVER (PARTITION BY MONTH(opened_on) ORDER BY median_move_qty DESC, id, move_id))) / 60, 1) AS minutes_since_prev_opened_on_year
    FROM compared
    WINDOW w_move_operator_move_type_by_m AS (PARTITION BY move_operator, move_type ORDER BY move_qty_minus_median DESC, id, move_id)
)
-- Subtotals with CUBE
SELECT
    move_id,
    move_type,
    move_qty_minus_median_dense_rank,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(capacity_bucket) / NULLIF(SUM(move_qty_minus_median_dense_rank), 0), 3) AS capacity_bucket_to_move_qty_minus_median_dense_r,
    MAX(capacity_bucket) AS max_capacity_bucket
FROM windowed
GROUP BY CUBE(move_id, move_type, move_qty_minus_median_dense_rank)
ORDER BY move_id NULLS LAST, move_type NULLS LAST, move_qty_minus_median_dense_rank NULLS LAST
