WITH valid_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM warehouses
    WHERE capacity < 11506
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS first_moved_at,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.id) AS total_move_unit_price,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count
    FROM valid_warehouses s
)
-- Subtotals with GROUPING SETS
SELECT
    move_count,
    city,
    DATEDIFF(MAX(opened_on), MIN(opened_on)) AS opened_on_span_days,
    MAX(first_moved_at) AS last_first_moved_at
FROM profiled
GROUP BY GROUPING SETS ((move_count, city), (move_count), (city), ())
ORDER BY move_count NULLS LAST, city NULLS LAST
