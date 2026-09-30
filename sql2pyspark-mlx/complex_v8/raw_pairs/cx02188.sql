WITH valid_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city = 'Singapore'
      AND LOWER(wh_code) LIKE '%x1%'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        FIRST_VALUE(opened_on) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_on,
        NTH_VALUE(opened_on, 2) OVER (PARTITION BY city ORDER BY opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_on,
        LAG(opened_on) OVER w_all_by_capacity AS prev_opened_on,
        capacity - LAG(capacity) OVER (ORDER BY capacity ASC NULLS LAST, id) AS capacity_change,
        NTH_VALUE(opened_on, 2) OVER (ORDER BY opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_on_2
    FROM valid_warehouses
    WINDOW w_all_by_capacity AS (ORDER BY capacity ASC NULLS LAST, id)
),
city_stats AS (
    -- Avg capacity per city
    SELECT city, AVG(capacity) AS avg_capacity
    FROM sequenced
    GROUP BY city
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_capacity,
        ROUND(s.capacity - g.avg_capacity, 2) AS capacity_minus_avg
    FROM sequenced s
    JOIN city_stats g ON g.city = s.city
    WHERE s.capacity > g.avg_capacity
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, capacity_minus_avg, avg_capacity
    FROM vs_group
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (capacity_minus_avg, avg_capacity)
) unp
