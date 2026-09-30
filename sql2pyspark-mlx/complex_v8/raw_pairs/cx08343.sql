WITH prepared_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        MONTH(opened_on) AS opened_on_month
    FROM warehouses
    WHERE LENGTH(TRIM(wh_code)) >= 8
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM prepared_warehouses s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    opened_on_month,
    move_count,
    COALESCE(city, 'ALL') AS city,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(move_count), 2) AS stddev_move_count,
    ARRAY_SORT(COLLECT_SET(city)) AS city_list,
    ROUND(SUM(move_count) / NULLIF(SUM(opened_on_month), 0), 3) AS move_count_to_opened_on_month_ratio
FROM with_counts
GROUP BY GROUPING SETS ((opened_on_month, move_count, city), (opened_on_month), ())
