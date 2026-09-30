WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DAYOFWEEK(opened_on) AS opened_on_dayofweek,
        SUBSTRING(TRIM(city), 1, 3) AS city_prefix
    FROM warehouses
    WHERE city IS NOT NULL
      AND LENGTH(TRIM(wh_code)) >= 8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        wh_code,
        opened_on,
        opened_on_dayofweek,
        MIN(capacity) OVER (PARTITION BY city, opened_on_dayofweek) AS city_opened_on_dayofweek_min_capacity,
        LAG(capacity, 2, 0) OVER w_opened_on_dayofweek_opened_o AS prev2_capacity,
        RANK() OVER w_opened_on_dayofweek_by_capac AS capacity_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY city ORDER BY opened_on_dayofweek ASC, id), 3) AS opened_on_dayofweek_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY city ORDER BY opened_on_dayofweek ASC), 3) AS opened_on_dayofweek_percent_rank_2,
        LAST_VALUE(opened_on_dayofweek) OVER (PARTITION BY city ORDER BY opened_on_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_on_dayofweek,
        RANK() OVER (PARTITION BY city ORDER BY opened_on_dayofweek ASC, id) AS opened_on_dayofweek_rank,
        ROUND(capacity - AVG(capacity) OVER w_city_opened_on_dayofweek, 2) AS capacity_vs_city_opened_on_dayofweek_avg,
        DATEDIFF(opened_on, LAG(opened_on) OVER (PARTITION BY opened_on_dayofweek, MONTH(opened_on) ORDER BY capacity ASC, id)) AS days_since_prev_opened_on,
        FIRST_VALUE(opened_on_dayofweek) OVER (PARTITION BY city ORDER BY opened_on_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_on_dayofweek,
        ROUND(AVG(opened_on_dayofweek) OVER (PARTITION BY city ORDER BY opened_on_dayofweek ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS opened_on_dayofweek_avg_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY opened_on_dayofweek ORDER BY capacity, id), 3) AS capacity_cume_dist,
        COUNT(*) OVER (PARTITION BY opened_on_dayofweek ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM filtered
    WINDOW w_opened_on_dayofweek_opened_o AS (PARTITION BY opened_on_dayofweek, MONTH(opened_on) ORDER BY capacity ASC, id), w_opened_on_dayofweek_by_capac AS (PARTITION BY opened_on_dayofweek ORDER BY capacity, id), w_city_opened_on_dayofweek AS (PARTITION BY city, opened_on_dayofweek)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE capacity_rank = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(opened_on, 2) OVER (PARTITION BY capacity_rank ORDER BY prev2_capacity ASC, id) AS next2_opened_on,
        MAX(capacity_cume_dist) OVER (PARTITION BY opened_on_dayofweek_rank, DATE_TRUNC('MONTH', opened_on) ORDER BY last_opened_on_dayofweek, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS capacity_cume_dist_max_3_prior,
        LAG(first_opened_on_dayofweek) OVER (PARTITION BY opened_on_dayofweek_rank, DATE_TRUNC('MONTH', opened_on) ORDER BY last_opened_on_dayofweek, id) AS prev_first_opened_on_dayofweek,
        COUNT(*) OVER (PARTITION BY YEAR(opened_on), first_opened_on_dayofweek) AS rows_in_opened_on_year_first_opened_on_dayofweek,
        RANK() OVER (PARTITION BY first_opened_on_dayofweek ORDER BY prev2_capacity NULLS LAST, id) AS prev2_capacity_rank
    FROM top_per_group
)
SELECT
    id,
    wh_code,
    opened_on_dayofweek,
    city_opened_on_dayofweek_min_capacity,
    prev2_capacity,
    capacity_rank,
    opened_on_dayofweek_percent_rank,
    opened_on_dayofweek_percent_rank_2,
    last_opened_on_dayofweek,
    opened_on_dayofweek_rank,
    capacity_vs_city_opened_on_dayofweek_avg,
    days_since_prev_opened_on,
    opened_on_dayofweek_avg_2_prior,
    capacity_cume_dist,
    running_count,
    next2_opened_on,
    capacity_cume_dist_max_3_prior,
    prev2_capacity_rank
FROM ranked
WHERE capacity_rank <= 9
ORDER BY days_since_prev_opened_on DESC
