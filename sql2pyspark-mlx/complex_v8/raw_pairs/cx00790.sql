WITH filtered_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity
    FROM warehouses
    WHERE city = 'Singapore'
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered_warehouses s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator IN ('op_bee', 'op_eve')
              AND st.sku_id <= 15
        )
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY city), 2) AS capacity_vs_city_avg,
        RANK() OVER w_city_by_capacity_ties AS capacity_rank,
        MAX(capacity) OVER (PARTITION BY city) AS city_max_capacity,
        FIRST_VALUE(capacity) IGNORE NULLS OVER (ORDER BY capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_capacity
    FROM with_activity
    WINDOW w_city_by_capacity_ties AS (PARTITION BY city ORDER BY capacity)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(wh_code, '#', CAST(city_max_capacity AS STRING)) AS wh_code_ref,
        LEAST(capacity_vs_city_avg, first_known_capacity) AS least_capacity_vs_city_avg_first_known_capacity
    FROM ranked
    WHERE capacity < 11768
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, city_max_capacity, capacity_rank, capacity
    FROM tagged
) src
UNPIVOT (
    reading FOR measure IN (city_max_capacity, capacity_rank, capacity)
) unp
