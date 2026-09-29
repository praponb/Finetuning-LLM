WITH filtered AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_to,
        rate
    FROM tariff_windows
    WHERE (rate < 5.7 OR MONTH(valid_to) IN (1, 3))
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', valid_to) AS valid_to_month,
        tariff,
        MAX(rate) AS max_rate,
        SUM(rate) AS total_rate,
        MIN(valid_to) AS first_valid_to,
        COUNT(*) AS row_count,
        ROUND(SUM(rate), 2) AS total_rate_2
    FROM filtered
    WHERE tariff <> 'TOU'
    GROUP BY DATE_TRUNC('MONTH', valid_to), tariff
),
annotated AS (
    -- Derive helper columns
    SELECT *, ABS(total_rate - 22.7) AS total_rate_distance
    FROM per_group
    WHERE row_count < 8
),
derived AS (
    -- Derive helper columns
    SELECT
        valid_to_month,
        tariff,
        max_rate,
        first_valid_to,
        row_count,
        total_rate_distance,
        COALESCE(total_rate_distance, 0.0) AS total_rate_distance_filled,
        CASE WHEN total_rate_distance >= 33.6 THEN 'HIGH' WHEN total_rate_distance >= 16.8 THEN 'MEDIUM' ELSE 'LOW' END AS total_rate_distance_tier,
        ABS(row_count - 8) AS row_count_distance
    FROM annotated
    WHERE max_rate > 2.6
),
total_rate_distance_tier_stats AS (
    -- Avg max_rate per total_rate_distance_tier
    SELECT total_rate_distance_tier, AVG(max_rate) AS avg_max_rate
    FROM derived
    GROUP BY total_rate_distance_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_max_rate,
        ROUND(s.max_rate - g.avg_max_rate, 2) AS max_rate_minus_avg
    FROM derived s
    JOIN total_rate_distance_tier_stats g ON g.total_rate_distance_tier = s.total_rate_distance_tier
    WHERE s.max_rate > g.avg_max_rate
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM vs_group
    WHERE max_rate_minus_avg > (SELECT AVG(max_rate_minus_avg) FROM vs_group)
)
SELECT *
FROM (
    SELECT tariff, row_count, total_rate_distance_tier, row_count_distance
    FROM with_activity
) src
PIVOT (
    AVG(row_count_distance)
    FOR total_rate_distance_tier IN ('HIGH', 'LOW')
)
