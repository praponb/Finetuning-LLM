WITH prepared_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        ABS(capacity - 11506) AS capacity_distance,
        SUBSTRING(TRIM(wh_code), 1, 3) AS wh_code_prefix
    FROM warehouses
    WHERE city = 'Singapore'
      AND opened_on < DATE '2025-07-03'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(capacity, 1, 0) OVER (PARTITION BY city ORDER BY capacity DESC, id) AS prev_capacity,
        FIRST_VALUE(capacity_distance) IGNORE NULLS OVER (PARTITION BY city ORDER BY capacity_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_capacity_distance,
        MIN(capacity_distance) OVER (PARTITION BY city) AS city_min_capacity_distance,
        MAX(capacity) OVER (PARTITION BY city) AS city_max_capacity,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY capacity DESC, id) AS capacity_dense_rank,
        DENSE_RANK() OVER w_city_by_capacity_distance AS capacity_distance_dense_rank,
        LAST_VALUE(capacity) OVER (PARTITION BY city ORDER BY capacity_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_capacity,
        ROUND(AVG(capacity) OVER (PARTITION BY city ORDER BY capacity_distance DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_7,
        ROUND(AVG(capacity_distance) OVER (PARTITION BY city), 2) AS city_avg_capacity_distance
    FROM prepared_warehouses
    WINDOW w_city_by_capacity_distance AS (PARTITION BY city ORDER BY capacity_distance ASC, id)
)
-- Final projection
SELECT
    id,
    capacity,
    wh_code_prefix,
    prev_capacity,
    city_min_capacity_distance,
    city_max_capacity,
    capacity_dense_rank,
    capacity_distance_dense_rank,
    last_capacity,
    capacity_avg_7,
    city_avg_capacity_distance,
    CONCAT_WS('|', wh_code, wh_code_prefix) AS wh_code_wh_code_prefix_label,
    LOWER(REGEXP_REPLACE(TRIM(wh_code_prefix), ' ', '_')) AS wh_code_prefix_slug
FROM sequenced
ORDER BY wh_code_prefix_slug DESC NULLS FIRST, prev_capacity DESC, id
LIMIT 5
