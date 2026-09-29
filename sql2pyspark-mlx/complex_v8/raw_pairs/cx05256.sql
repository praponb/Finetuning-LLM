WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        CONCAT(wh_code, '#', CAST(capacity AS STRING)) AS wh_code_ref,
        CASE WHEN capacity >= 11768 THEN 'large' WHEN capacity >= 4912 THEN 'medium' ELSE 'small' END AS capacity_tier
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%bk%'
),
capacity_tier_stats AS (
    -- Median capacity per capacity_tier
    SELECT capacity_tier, PERCENTILE_APPROX(capacity, 0.5) AS median_capacity
    FROM cleaned_warehouses
    GROUP BY capacity_tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_capacity,
        ROUND(s.capacity - g.median_capacity, 2) AS capacity_minus_median
    FROM cleaned_warehouses s
    JOIN capacity_tier_stats g ON g.capacity_tier = s.capacity_tier
    WHERE s.capacity >= g.median_capacity
),
capacity_tier_stats_2 AS (
    -- Median capacity_minus_median per capacity_tier
    SELECT capacity_tier, PERCENTILE_APPROX(capacity_minus_median, 0.5) AS median_capacity_minus_median
    FROM outliers
    GROUP BY capacity_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_capacity_minus_median,
        ROUND(s.capacity_minus_median - g.median_capacity_minus_median, 2) AS capacity_minus_median_minus_median
    FROM outliers s
    JOIN capacity_tier_stats_2 g ON g.capacity_tier = s.capacity_tier
    WHERE s.capacity_minus_median >= g.median_capacity_minus_median
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        wh_code,
        opened_on,
        wh_code_ref,
        capacity_tier,
        capacity_minus_median,
        median_capacity_minus_median,
        capacity_minus_median_minus_median,
        CASE WHEN median_capacity >= 13350.0 THEN 'large' WHEN median_capacity >= 6700.0 THEN 'medium' ELSE 'small' END AS median_capacity_tier,
        CONCAT(wh_code_ref, '#', CAST(capacity AS STRING)) AS wh_code_ref_ref,
        CASE WHEN capacity_tier IN ('large', 'medium') THEN 'group_a' WHEN capacity_tier = 'small' THEN 'group_b' ELSE 'other' END AS capacity_tier_group
    FROM above_average
    WHERE opened_on >= DATE '2024-11-25'
)
SELECT *
FROM (
    SELECT opened_on, median_capacity_tier, capacity_tier_group, capacity_minus_median_minus_median
    FROM scored
) src
PIVOT (
    AVG(capacity_minus_median_minus_median) AS avg_v, COUNT(capacity_minus_median_minus_median) AS n
    FOR capacity_tier_group IN ('group_b', 'other')
)
ORDER BY opened_on, median_capacity_tier
