WITH recent_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        DATE_TRUNC('WEEK', valid_to) AS valid_to_week,
        ABS(rate - 5.5) AS rate_distance,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM tariff_windows
    WHERE (tariff = 'TOU' OR valid_to >= TIMESTAMP '2025-02-16 05:00:00')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(valid_to) AS valid_to_day,
        DATE_TRUNC('MONTH', valid_to_week) AS valid_to_week_month,
        CASE WHEN rate_distance >= 4.4 THEN 'large' WHEN rate_distance >= 3.6 THEN 'medium' ELSE 'small' END AS rate_distance_tier,
        MAX(valid_from) AS last_valid_from,
        PERCENTILE_APPROX(rate_distance, 0.5) AS median_rate_distance,
        COUNT(DISTINCT id) AS distinct_ids
    FROM recent_tariffs
    GROUP BY TO_DATE(valid_to), DATE_TRUNC('MONTH', valid_to_week), CASE WHEN rate_distance >= 4.4 THEN 'large' WHEN rate_distance >= 3.6 THEN 'medium' ELSE 'small' END
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        valid_to_day,
        valid_to_week_month,
        rate_distance_tier,
        median_rate_distance,
        distinct_ids,
        SUM(distinct_ids) OVER (PARTITION BY MONTH(last_valid_from), rate_distance_tier ORDER BY distinct_ids DESC, valid_to_day, valid_to_week_month, rate_distance_tier ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS distinct_ids_sum_2_prior,
        FIRST_VALUE(last_valid_from) IGNORE NULLS OVER (PARTITION BY MONTH(last_valid_from), rate_distance_tier ORDER BY distinct_ids DESC, valid_to_day, valid_to_week_month, rate_distance_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_valid_from,
        RANK() OVER (PARTITION BY MONTH(last_valid_from), rate_distance_tier ORDER BY distinct_ids DESC) AS distinct_ids_rank
    FROM stats
)
-- Final projection
SELECT
    valid_to_day,
    valid_to_week_month,
    rate_distance_tier,
    median_rate_distance,
    distinct_ids,
    distinct_ids_rank,
    LOWER(TRIM(rate_distance_tier)) AS rate_distance_tier_clean
FROM with_trends
ORDER BY distinct_ids DESC, valid_to_day, valid_to_week_month, rate_distance_tier
LIMIT 20
