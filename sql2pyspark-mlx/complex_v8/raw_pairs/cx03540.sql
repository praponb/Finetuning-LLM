WITH filtered_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN city IN ('Bangkok', 'Singapore') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group
    FROM warehouses
),
enriched AS (
    -- Label and score rows
    SELECT *, COALESCE(capacity, 0) AS capacity_filled
    FROM filtered_warehouses
    WHERE YEAR(opened_on) = 2024
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(capacity_filled) OVER w_city_city_group AS city_city_group_sum_capacity_filled,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY city, city_group), 0), 4) AS capacity_share_of_city_city_group,
        COUNT(*) OVER w_city_city_group AS rows_in_city_city_group,
        SUM(capacity) OVER (ORDER BY capacity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_capacity,
        LEAD(capacity_filled) OVER w_all_by_capacity_filled AS next_capacity_filled
    FROM enriched
    WINDOW w_city_city_group AS (PARTITION BY city, city_group), w_all_by_capacity_filled AS (ORDER BY capacity_filled, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_city_group_by_capacity AS capacity_dense_rank,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY city_group, city), 0), 4) AS capacity_share_of_city_group_city
    FROM sequenced
    WINDOW w_city_group_by_capacity AS (PARTITION BY city_group ORDER BY capacity ASC, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(capacity_dense_rank) OVER w_opened_on_month_by_rows_in_c AS prev_capacity_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on) ORDER BY rows_in_city_city_group, id), 3) AS rows_in_city_city_group_cume_dist,
        SIZE(COLLECT_SET(city_group) OVER (PARTITION BY city)) AS distinct_city_group_in_city,
        ROUND(AVG(capacity_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on) ORDER BY rows_in_city_city_group, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS capacity_filled_avg_7
    FROM metrics
    WINDOW w_opened_on_month_by_rows_in_c AS (PARTITION BY DATE_TRUNC('MONTH', opened_on) ORDER BY rows_in_city_city_group, id)
),
latest_per_key AS (
    -- Keep the latest row per city, city_group
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_group,
        capacity_filled,
        city_city_group_sum_capacity_filled,
        capacity_share_of_city_city_group,
        rows_in_city_city_group,
        running_capacity,
        next_capacity_filled,
        capacity_dense_rank,
        capacity_share_of_city_group_city,
        prev_capacity_dense_rank,
        rows_in_city_city_group_cume_dist,
        distinct_city_group_in_city,
        capacity_filled_avg_7
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city, city_group ORDER BY capacity_dense_rank DESC, id DESC) AS recency_rank
        FROM windowed
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    capacity,
    opened_on,
    capacity_filled,
    city_city_group_sum_capacity_filled,
    capacity_share_of_city_city_group,
    rows_in_city_city_group,
    next_capacity_filled,
    capacity_dense_rank,
    capacity_share_of_city_group_city,
    rows_in_city_city_group_cume_dist,
    distinct_city_group_in_city
FROM latest_per_key
ORDER BY id
