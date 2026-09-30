WITH base AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_TRUNC('WEEK', opened_on) AS opened_on_week
    FROM warehouses
    WHERE capacity < 11768
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(opened_on_week, 2) OVER (PARTITION BY city, MONTH(opened_on) ORDER BY capacity, id) AS next2_opened_on_week,
        SUM(capacity) OVER w_opened_on_week_month AS opened_on_week_month_sum_capacity,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY MONTH(opened_on_week)), 0), 4) AS capacity_share_of_opened_on_week_month,
        COUNT(*) OVER w_opened_on_week_month AS rows_in_opened_on_week_month,
        DENSE_RANK() OVER w_all_by_opened_on_week_ties AS opened_on_week_dense_rank,
        FIRST_VALUE(capacity) OVER (PARTITION BY MONTH(opened_on), city ORDER BY capacity ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_capacity,
        DENSE_RANK() OVER (PARTITION BY MONTH(opened_on), city ORDER BY capacity ASC NULLS LAST, id) AS capacity_dense_rank,
        NTILE(4) OVER (ORDER BY opened_on_week, id) AS opened_on_week_quartile,
        ROUND(AVG(capacity) OVER (PARTITION BY city), 2) AS city_avg_capacity,
        NTILE(5) OVER (ORDER BY opened_on_week, id) AS opened_on_week_quintile,
        RANK() OVER (ORDER BY opened_on_week, id) AS opened_on_week_rank,
        LEAD(opened_on, 2) OVER (PARTITION BY city, MONTH(opened_on) ORDER BY capacity, id) AS next2_opened_on,
        NTH_VALUE(capacity, 2) OVER (PARTITION BY MONTH(opened_on), city ORDER BY capacity ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity
    FROM base
    WINDOW w_opened_on_week_month AS (PARTITION BY MONTH(opened_on_week)), w_all_by_opened_on_week_ties AS (ORDER BY opened_on_week)
),
per_group AS (
    -- Group-level totals
    SELECT
        opened_on_week_quintile,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(opened_on_week_quintile) AS max_opened_on_week_quintile,
        ROUND(AVG(opened_on_week_month_sum_capacity), 2) AS avg_opened_on_week_month_sum_capacity,
        SUM(opened_on_week_dense_rank) AS total_opened_on_week_dense_rank
    FROM sequenced
    GROUP BY opened_on_week_quintile
    HAVING SUM(second_capacity) > 2614
)
SELECT
    opened_on_week_quintile,
    distinct_ids,
    max_opened_on_week_quintile,
    total_opened_on_week_dense_rank,
    ROUND(opened_on_week_quintile / NULLIF(distinct_ids, 0), 3) AS opened_on_week_quintile_per_distinct_ids
FROM per_group
ORDER BY distinct_ids
