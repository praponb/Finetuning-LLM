WITH scoped_warehouses AS (
    -- Scope the warehouses rows
    SELECT *
    FROM warehouses
    WHERE wh_code IS NOT NULL
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM scoped_warehouses s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.unit_price <= 292.3
              AND st.qty < 256
        )
      AND s.city = 'Bangkok'
),
city_stats AS (
    -- Max move_count per city
    SELECT city, MAX(move_count) AS max_move_count
    FROM qualified
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_move_count,
        ROUND(s.move_count - g.max_move_count, 2) AS move_count_minus_max
    FROM qualified s
    JOIN city_stats g ON g.city = s.city
    WHERE s.move_count = g.max_move_count
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_count, city, capacity
    FROM outliers
) src
PIVOT (
    MIN(capacity)
    FOR city IN ('Singapore', 'Bangkok')
)
