WITH cleaned AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE opened_on >= DATE '2024-11-25'
      AND city IN ('Singapore', 'Kuala Lumpur')
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MIN(unit_price) AS min_move_unit_price,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.min_move_unit_price,
        COALESCE(st.move_count, 0) AS move_count
    FROM cleaned s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM with_stats
    WHERE move_count > (SELECT MIN(move_count) FROM with_stats)
)
SELECT
    move_count,
    city,
    COUNT(DISTINCT city) AS distinct_citys
FROM with_activity
GROUP BY CUBE(move_count, city)
ORDER BY move_count NULLS LAST, city NULLS LAST
