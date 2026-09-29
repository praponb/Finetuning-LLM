WITH base AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        UPPER(TRIM(city)) AS city_clean
    FROM warehouses
    WHERE opened_on >= DATE '2024-11-25'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        capacity,
        city_clean,
        ROUND(capacity / NULLIF(SUM(capacity) OVER w_opened_on_year, 0), 4) AS capacity_share_of_opened_on_year,
        ROUND(AVG(capacity) OVER (PARTITION BY city ORDER BY capacity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_4,
        ROUND(CUME_DIST() OVER (PARTITION BY city ORDER BY capacity DESC, id), 3) AS capacity_cume_dist,
        MAX(capacity) OVER (PARTITION BY YEAR(opened_on)) AS opened_on_year_max_capacity
    FROM base
    WINDOW w_opened_on_year AS (PARTITION BY YEAR(opened_on))
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        capacity,
        capacity_share_of_opened_on_year,
        ROW_NUMBER() OVER (ORDER BY capacity_share_of_opened_on_year DESC, id) AS capacity_share_of_opened_on_year_seq,
        SUM(capacity) OVER (ORDER BY opened_on_year_max_capacity NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_capacity,
        ROUND(AVG(capacity_cume_dist) OVER (ORDER BY opened_on_year_max_capacity NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS capacity_cume_dist_avg_4,
        ROUND(AVG(capacity) OVER (ORDER BY capacity_avg_4, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_3,
        FIRST_VALUE(opened_on_year_max_capacity) OVER (ORDER BY capacity_avg_4, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_on_year_max_capacity
    FROM ranked
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY capacity_share_of_opened_on_year_seq ORDER BY capacity DESC, id) AS capacity_rank,
        ROUND(capacity_avg_3 / NULLIF(SUM(capacity_avg_3) OVER (PARTITION BY capacity_share_of_opened_on_year_seq), 0), 4) AS capacity_avg_3_share_of_capacity_share_of_opened,
        DENSE_RANK() OVER w_all_by_capacity_share_of_ope_2 AS capacity_share_of_opened_on_year_dense_rank
    FROM with_windows
    WINDOW w_all_by_capacity_share_of_ope_2 AS (ORDER BY capacity_share_of_opened_on_year ASC NULLS LAST)
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(capacity_cume_dist_avg_4) OVER (PARTITION BY capacity_rank, capacity_share_of_opened_on_year_seq ORDER BY capacity_cume_dist_avg_4 DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS capacity_cume_dist_avg_4_avg_2,
        ROUND(AVG(capacity_avg_3) OVER (PARTITION BY capacity_share_of_opened_on_year_seq, capacity_share_of_opened_on_year_dense_rank ORDER BY first_opened_on_year_max_capacity DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS capacity_avg_3_avg_4,
        NTH_VALUE(capacity_avg_3, 2) OVER (PARTITION BY capacity_rank, capacity_share_of_opened_on_year_seq ORDER BY capacity_avg_3_share_of_capacity_share_of_opened, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity_avg_3
    FROM with_trends
)
-- Final projection
SELECT
    id,
    capacity,
    capacity_share_of_opened_on_year_seq,
    running_capacity,
    capacity_avg_3_share_of_capacity_share_of_opened,
    capacity_cume_dist_avg_4_avg_2,
    second_capacity_avg_3,
    capacity_avg_3_avg_4 - second_capacity_avg_3 AS capacity_avg_3_avg_4_minus_second_capacity_avg_3,
    ROUND(capacity_rank / NULLIF(capacity_cume_dist_avg_4, 0), 3) AS capacity_rank_per_capacity_cume_dist_avg_4
FROM with_windows_2
ORDER BY capacity_rank_per_capacity_cume_dist_avg_4, capacity
