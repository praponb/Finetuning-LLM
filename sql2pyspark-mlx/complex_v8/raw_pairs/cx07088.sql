WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE operator = 'op_bee'
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id IS NOT NULL
              AND st.operator IN ('op_bee', 'op_ann', 'op_dew')
        )
      AND s.move_type = 'OUTBOUND'
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.operator IN ('op_ann', 'op_eve', 'op_chan') THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count
    FROM with_activity s
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        moved_at_dayofweek,
        FIRST_VALUE(moved_at_dayofweek) IGNORE NULLS OVER (PARTITION BY move_operator_count ORDER BY move_operator_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at_dayofweek,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY moved_at_dayofweek)) AS distinct_sku_id_in_moved_at_dayofweek,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_all_by_moved_at_dayofweek)) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(greatest_unit_price_qty / NULLIF(SUM(greatest_unit_price_qty) OVER w_moved_at_dayofweek, 0), 4) AS greatest_unit_price_qty_share_of_moved_at_dayofw,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at_dayofweek DESC), 3) AS moved_at_dayofweek_cume_dist
    FROM summarized_rows
    WINDOW w_all_by_moved_at_dayofweek AS (ORDER BY moved_at_dayofweek DESC, id), w_moved_at_dayofweek AS (PARTITION BY moved_at_dayofweek)
),
move_type_stats AS (
    -- Avg minutes_since_prev_moved_at per move_type
    SELECT move_type, AVG(minutes_since_prev_moved_at) AS avg_minutes_since_prev_moved_at
    FROM windowed
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_minutes_since_prev_moved_at,
        ROUND(s.minutes_since_prev_moved_at - g.avg_minutes_since_prev_moved_at, 2) AS minutes_since_prev_moved_at_minus_avg
    FROM windowed s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.minutes_since_prev_moved_at > g.avg_minutes_since_prev_moved_at
)
-- Final projection
SELECT
    id,
    move_type,
    moved_at,
    moved_at_dayofweek,
    first_known_moved_at_dayofweek,
    distinct_sku_id_in_moved_at_dayofweek,
    minutes_since_prev_moved_at,
    greatest_unit_price_qty_share_of_moved_at_dayofw,
    minutes_since_prev_moved_at_minus_avg
FROM outliers
ORDER BY greatest_unit_price_qty_share_of_moved_at_dayofw, id
LIMIT 5
