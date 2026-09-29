WITH filtered AS (
    -- Scope the warehouses rows
    SELECT
        id,
        city,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled,
        DATE_FORMAT(opened_on, 'MMM yyyy') AS opened_on_label
    FROM warehouses
    WHERE capacity <= 11506
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        city,
        capacity,
        opened_on,
        ROUND(AVG(capacity) OVER (PARTITION BY city ORDER BY capacity_filled NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS capacity_avg_1_prior,
        LAG(opened_on) OVER w_all_by_capacity_filled AS prev_opened_on
    FROM filtered
    WINDOW w_all_by_capacity_filled AS (ORDER BY capacity_filled, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_ym,
        ARRAY_SORT(COLLECT_SET(city)) AS city_list,
        COUNT(*) FILTER (WHERE city IS NOT NULL) AS n_city_is_not_null,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count
    FROM sequenced
    GROUP BY 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        opened_on_ym,
        n_city_is_not_null,
        distinct_ids,
        row_count,
        ROUND(AVG(distinct_ids) OVER (PARTITION BY n_city_is_not_null, distinct_ids ORDER BY distinct_ids DESC, opened_on_ym ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_ids_avg_1_prior,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY n_city_is_not_null, distinct_ids ORDER BY distinct_ids DESC, opened_on_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        RANK() OVER w_row_count_distinct_ids_by_ro AS row_count_rank,
        LAST_VALUE(n_city_is_not_null) OVER (PARTITION BY row_count, distinct_ids ORDER BY row_count DESC, opened_on_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_n_city_is_not_null,
        LAG(distinct_ids) OVER (PARTITION BY n_city_is_not_null, distinct_ids ORDER BY distinct_ids DESC, opened_on_ym) AS prev_distinct_ids
    FROM grouped
    WINDOW w_row_count_distinct_ids_by_ro AS (PARTITION BY row_count, distinct_ids ORDER BY row_count DESC)
)
SELECT
    opened_on_ym,
    distinct_ids,
    row_count,
    distinct_ids_avg_1_prior,
    row_count_rank,
    last_n_city_is_not_null,
    prev_distinct_ids,
    FLOOR(n_city_is_not_null / 2) * 2 AS n_city_is_not_null_bucket
FROM with_trends
ORDER BY distinct_ids_avg_1_prior NULLS LAST
