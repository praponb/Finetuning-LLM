WITH base_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM warehouses
    WHERE opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-12-17'
      AND capacity <= 11768
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS first_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count
    FROM base_warehouses s
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count_2
    FROM summarized_rows s
)
SELECT
    move_count,
    city,
    SUM(move_count_2) AS total_move_count_2,
    SUM(capacity) AS total_capacity,
    ARRAY_SORT(COLLECT_SET(city)) AS city_list
FROM with_counts
GROUP BY GROUPING SETS ((move_count, city), (move_count), (city), ())
ORDER BY move_count NULLS LAST, city NULLS LAST
