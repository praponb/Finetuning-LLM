WITH recent AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city IN ('Kuala Lumpur', 'Singapore')
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        MIN(unit_price) AS min_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.min_move_unit_price
    FROM recent s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        city,
        capacity,
        opened_on,
        move_count,
        min_move_unit_price,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug
    FROM with_stats
    WHERE LOWER(wh_code) LIKE '%ul%'
)
-- Subtotals with CUBE
SELECT
    move_count,
    city,
    ARRAY_SORT(COLLECT_SET(city)) AS city_list,
    SUM(move_count) AS total_move_count
FROM derived
GROUP BY CUBE(move_count, city)
ORDER BY move_count NULLS LAST, city NULLS LAST
