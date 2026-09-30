WITH recent AS (
    -- Filter warehouses
    SELECT *
    FROM warehouses
    WHERE capacity < 11506
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(capacity, 1, 0) OVER w_city_by_opened_on AS prev_capacity,
        NTILE(3) OVER (PARTITION BY city ORDER BY capacity DESC, id) AS capacity_tercile,
        ROUND(AVG(capacity) OVER (PARTITION BY city), 2) AS city_avg_capacity,
        ROUND(AVG(capacity) OVER (PARTITION BY city ORDER BY opened_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS capacity_avg_1_prior
    FROM recent
    WINDOW w_city_by_opened_on AS (PARTITION BY city ORDER BY opened_on DESC, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(capacity_tercile) OVER (PARTITION BY capacity_tercile ORDER BY opened_on ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS capacity_tercile_max_2,
        ROUND(capacity_tercile - AVG(capacity_tercile) OVER (PARTITION BY city, capacity_tercile), 2) AS capacity_tercile_vs_city_capacity_tercile_avg
    FROM with_trends
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.capacity,
        s.opened_on,
        s.capacity_tercile,
        s.city_avg_capacity,
        s.capacity_avg_1_prior,
        s.capacity_tercile_max_2,
        s.capacity_tercile_vs_city_capacity_tercile_avg,
        st.id AS move_id,
        st.move_type,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator
    FROM sequenced s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.id) AS total_move_qty,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS first_moved_at
    FROM line_level_moves s
),
enriched AS (
    -- Attach move attributes
    SELECT
        s.id,
        s.capacity,
        s.opened_on,
        s.city_avg_capacity,
        s.capacity_avg_1_prior,
        s.capacity_tercile_max_2,
        s.capacity_tercile_vs_city_capacity_tercile_avg,
        s.move_id,
        s.move_type,
        s.move_moved_at,
        s.first_moved_at,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty
    FROM with_stats s
    LEFT JOIN stock_moves st ON st.id = s.move_id
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator IN ('op_dew', 'op_bee', 'op_eve')
              AND st.qty > 198
        )
)
-- Final projection
SELECT
    id,
    capacity,
    city_avg_capacity,
    capacity_avg_1_prior,
    move_id,
    move_moved_at,
    first_moved_at,
    move_unit_price,
    move_qty
FROM filtered
ORDER BY id DESC NULLS FIRST
