WITH prepared_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        city,
        capacity,
        opened_on,
        QUARTER(opened_on) AS opened_on_quarter
    FROM warehouses
    WHERE opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-07-03'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(city) OVER (PARTITION BY opened_on_quarter, MONTH(opened_on))) AS distinct_city_in_opened_on_quarter_opened_on_mon,
        ROUND(CUME_DIST() OVER (ORDER BY opened_on ASC, id), 3) AS opened_on_cume_dist,
        ROUND(AVG(capacity) OVER (PARTITION BY opened_on_quarter, city ORDER BY capacity ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS capacity_avg_6_prior,
        DENSE_RANK() OVER w_all_by_opened_on_ties AS opened_on_dense_rank,
        LEAD(capacity, 3) OVER (PARTITION BY opened_on_quarter, city ORDER BY opened_on ASC, id) AS next3_capacity,
        RANK() OVER (PARTITION BY opened_on_quarter, city ORDER BY capacity ASC NULLS LAST) AS capacity_rank
    FROM prepared_warehouses
    WINDOW w_all_by_opened_on_ties AS (ORDER BY opened_on ASC), w_opened_on_quarter_opened_on AS (PARTITION BY opened_on_quarter, MONTH(opened_on))
)
-- Final projection
SELECT
    id,
    city,
    opened_on,
    distinct_city_in_opened_on_quarter_opened_on_mon,
    opened_on_cume_dist,
    capacity_avg_6_prior,
    opened_on_dense_rank,
    next3_capacity,
    capacity_rank,
    DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
    CASE WHEN city IN ('Bangkok', 'Kuala Lumpur') THEN 'group_a' WHEN city = 'Singapore' THEN 'group_b' ELSE 'other' END AS city_group
FROM with_windows
ORDER BY opened_on_cume_dist, id DESC NULLS FIRST
