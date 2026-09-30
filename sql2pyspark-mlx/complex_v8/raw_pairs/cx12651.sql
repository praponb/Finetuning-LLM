WITH filtered_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        ADD_MONTHS(opened_on, 3) AS opened_on_plus_3m,
        CASE WHEN city IN ('Singapore', 'Kuala Lumpur') THEN 'group_a' WHEN city = 'Bangkok' THEN 'group_b' ELSE 'other' END AS city_group,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
    WHERE city NOT IN ('Bangkok', 'Singapore')
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        COUNT(CASE WHEN operator IN ('op_ann', 'op_dew', 'op_bee') THEN 1 END) AS move_operator_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM filtered_warehouses s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
city_stats AS (
    -- Avg move_count per city
    SELECT city, AVG(move_count) AS avg_move_count
    FROM with_stats
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_move_count,
        ROUND(s.move_count - g.avg_move_count, 2) AS move_count_minus_avg
    FROM with_stats s
    JOIN city_stats g ON g.city = s.city
    WHERE s.move_count > g.avg_move_count
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', city, city_group) AS city_city_group_label,
        ABS(capacity_filled - 9550) AS capacity_filled_distance,
        TRUNC(opened_on_plus_3m, 'MM') AS opened_on_plus_3m_month_start
    FROM outliers
)
SELECT id, measure, amount
FROM (
    SELECT id, move_count_minus_avg, avg_move_count
    FROM annotated
) src
UNPIVOT (
    amount FOR measure IN (move_count_minus_avg, avg_move_count)
) unp
ORDER BY id, measure
