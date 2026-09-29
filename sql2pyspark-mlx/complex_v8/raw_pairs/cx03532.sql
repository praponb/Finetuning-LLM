WITH recent AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM warehouses
    WHERE capacity >= 2614
      AND opened_on < DATE '2025-02-14'
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS last_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS last_moved_at_2
    FROM recent s
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(CASE WHEN move_type IS NOT NULL THEN 1 END) AS move_move_type_count,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MAX(moved_at) AS last_moved_at_3
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count,
        st.avg_move_unit_price,
        st.last_moved_at_3
    FROM with_counts s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.id
)
SELECT
    move_count,
    move_move_type_count,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM profiled
GROUP BY GROUPING SETS ((move_count, move_move_type_count), (move_count), (move_move_type_count))
