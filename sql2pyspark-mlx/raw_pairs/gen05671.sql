WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(city) AS city_lower
    FROM stores
    WHERE region IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(sqft) OVER w_grp AS region_sum_sqft,
        FIRST_VALUE(region, TRUE) OVER w_ordered AS first_known_region
    FROM filtered
    WHERE opened_date > '2025-10-01'
    WINDOW w_grp AS (PARTITION BY region), w_ordered AS (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        city_lower,
        first_known_region,
        MAX(region_sum_sqft) OVER w_grp AS running_max_region_sum_sqft,
        AVG(region_sum_sqft) OVER w_grp AS running_avg_region_sum_sqft,
        SUM(region_sum_sqft) OVER w_grp AS running_sum_region_sum_sqft,
        COUNT(region_sum_sqft) OVER w_grp AS running_count_region_sum_sqft,
        SUM(sqft) OVER (PARTITION BY city, region ORDER BY region_sum_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY city ORDER BY region_sum_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(running_sum_region_sum_sqft, TRUE) OVER (ORDER BY running_sum_region_sum_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sum_region_sum_sqft,
        FIRST_VALUE(running_sqft) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sqft,
        LEAD(sqft, 2) OVER (PARTITION BY MONTH(opened_date) ORDER BY running_max_region_sum_sqft, id) AS next_sqft,
        RANK() OVER (ORDER BY running_count_region_sum_sqft DESC NULLS LAST) AS running_count_region_sum_sqft_rank
    FROM scored
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_max_region_sum_sqft) OVER (ORDER BY running_count_region_sum_sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_max_region_sum_sqft_avg_2,
        AVG(running_max_region_sum_sqft) OVER (ORDER BY running_sqft, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_max_region_sum_sqft_avg_3_prior,
        FIRST_VALUE(running_count_region_sum_sqft, TRUE) OVER (ORDER BY running_avg_region_sum_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count_region_sum_sqft,
        NTILE(10) OVER (PARTITION BY MONTH(opened_date) ORDER BY running_avg_region_sum_sqft, id) AS running_avg_region_sum_sqft_bucket,
        LAST_VALUE(running_count_region_sum_sqft, TRUE) OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date) ORDER BY running_max_region_sum_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_count_region_sum_sqft
    FROM with_windows_stores
)
SELECT
    id,
    store_name,
    opened_date,
    sqft,
    first_known_region,
    running_max_region_sum_sqft,
    running_avg_region_sum_sqft,
    running_count_region_sum_sqft,
    first_known_running_sqft
FROM enriched
ORDER BY id DESC
