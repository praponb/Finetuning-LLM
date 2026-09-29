WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        UPPER(TRIM(city)) AS city_clean
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
      AND opened_on >= DATE '2024-04-13'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_clean,
        LAST_VALUE(opened_on) OVER (PARTITION BY city ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_on,
        ROUND(CUME_DIST() OVER (PARTITION BY city ORDER BY capacity), 3) AS capacity_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY capacity DESC, id), 3) AS capacity_cume_dist_2,
        NTH_VALUE(opened_on, 2) OVER (ORDER BY capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_on
    FROM prepared
)
SELECT id, field, reading
FROM (
    SELECT id, capacity_cume_dist, capacity_cume_dist_2
    FROM metrics
) src
UNPIVOT (
    reading FOR field IN (capacity_cume_dist, capacity_cume_dist_2)
) unp
ORDER BY id, field
