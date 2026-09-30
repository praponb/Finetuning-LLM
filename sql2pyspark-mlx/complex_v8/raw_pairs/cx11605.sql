WITH scoped AS (
    -- Filter warehouses
    SELECT
        id,
        city,
        capacity,
        opened_on,
        DAYOFWEEK(opened_on) AS opened_on_dayofweek,
        CASE WHEN capacity >= 11506 THEN 'A' WHEN capacity >= 4912 THEN 'B' ELSE 'C' END AS capacity_tier,
        CASE WHEN city IN ('Singapore', 'Kuala Lumpur') THEN 'group_a' WHEN city = 'Bangkok' THEN 'group_b' ELSE 'other' END AS city_group
    FROM warehouses
    WHERE city = 'Kuala Lumpur'
),
city_stats AS (
    -- Median opened_on_dayofweek per city
    SELECT city, PERCENTILE_APPROX(opened_on_dayofweek, 0.5) AS median_opened_on_dayofweek
    FROM scoped
    GROUP BY city
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_opened_on_dayofweek,
        ROUND(s.opened_on_dayofweek - g.median_opened_on_dayofweek, 2) AS opened_on_dayofweek_minus_median
    FROM scoped s
    JOIN city_stats g ON g.city = s.city
    WHERE s.opened_on_dayofweek >= g.median_opened_on_dayofweek
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY city_group, opened_on_dayofweek) AS rows_in_city_group_opened_on_dayofweek,
        ROUND(opened_on_dayofweek - AVG(opened_on_dayofweek) OVER (PARTITION BY city_group, opened_on_dayofweek), 2) AS opened_on_dayofweek_vs_city_group_opened_on_dayo,
        FIRST_VALUE(opened_on_dayofweek) IGNORE NULLS OVER (ORDER BY capacity ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_on_dayofweek,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY city_group, opened_on_dayofweek), 2) AS capacity_vs_city_group_opened_on_dayofweek_avg,
        MAX(opened_on_dayofweek) OVER w_city_group_opened_on_dayofwe AS city_group_opened_on_dayofweek_max_opened_on_day,
        NTILE(3) OVER (ORDER BY capacity ASC, id) AS capacity_tercile
    FROM compared
    WINDOW w_city_group_opened_on_dayofwe AS (PARTITION BY city_group, opened_on_dayofweek)
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
        )
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.median_opened_on_dayofweek,
        s.first_known_opened_on_dayofweek,
        s.capacity_vs_city_group_opened_on_dayofweek_avg,
        s.city_group_opened_on_dayofweek_max_opened_on_day,
        st.id AS move_id,
        st.move_type,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty
    FROM filtered s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
filtered_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.unit_price BETWEEN 95.8 AND 412.4
              AND st.move_type IN ('TRANSFER', 'OUTBOUND')
        )
)
-- Final projection
SELECT
    id,
    median_opened_on_dayofweek,
    first_known_opened_on_dayofweek,
    capacity_vs_city_group_opened_on_dayofweek_avg,
    city_group_opened_on_dayofweek_max_opened_on_day,
    move_id,
    move_type,
    move_operator,
    move_moved_at,
    move_qty,
    ABS(move_qty - 216) AS move_qty_distance
FROM filtered_2
WHERE capacity_vs_city_group_opened_on_dayofweek_avg <= 12000.0
