WITH prepared AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE wh_code IS NOT NULL
      AND capacity <= 11768
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, SUM(qty) AS total_move_qty, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.total_move_qty,
        COALESCE(st.move_count, 0) AS move_count
    FROM prepared s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        city,
        capacity,
        total_move_qty,
        move_count,
        LEAD(capacity) OVER w_move_count_by_capacity AS next_capacity,
        LAG(capacity) OVER (PARTITION BY move_count ORDER BY capacity DESC NULLS FIRST, id) AS prev_capacity,
        COUNT(*) OVER (PARTITION BY city, MONTH(opened_on)) AS rows_in_city_opened_on_month,
        SUM(move_count) OVER (PARTITION BY city, MONTH(opened_on)) AS city_opened_on_month_sum_move_count,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(opened_on) ORDER BY move_count DESC), 3) AS move_count_cume_dist
    FROM profiled
    WINDOW w_move_count_by_capacity AS (PARTITION BY move_count ORDER BY capacity DESC NULLS FIRST, id)
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, ROUND(AVG(unit_price), 2) AS avg_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price
    FROM ranked s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.id
),
summary AS (
    -- Group-level totals
    SELECT
        city,
        ARRAY_SORT(COLLECT_SET(city)) AS city_list,
        SUM(CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END) AS n_city_is_not_null,
        COUNT(DISTINCT city) AS distinct_citys,
        COUNT(*) FILTER (WHERE city <> 'Singapore') AS n_city_singapore,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_counts
    WHERE next_capacity >= 2614
    GROUP BY city
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM summary
    WHERE distinct_citys < (SELECT MAX(distinct_citys) FROM summary)
)
-- Final projection
SELECT
    city,
    city_list,
    n_city_is_not_null,
    distinct_citys,
    n_city_singapore,
    cnt,
    distinct_ids,
    LEAST(n_city_singapore, distinct_citys) AS least_n_city_singapore_distinct_citys
FROM screened
WHERE n_city_is_not_null > 2
