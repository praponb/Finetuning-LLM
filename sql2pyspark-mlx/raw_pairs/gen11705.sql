WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        firmware,
        CASE WHEN device_code IN ('DV-01', 'DV-04') THEN 'Tier 1' ELSE 'Other' END AS device_code_group
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(humidity) IGNORE NULLS OVER w_ordered AS last_known_humidity,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity,
        AVG(humidity) OVER w_running AS humidity_avg_2,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY humidity NULLS FIRST) AS humidity_dense_rank
    FROM base
    WHERE firmware IN ('1.0.3', '1.1.0', '2.0.1')
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY DATE_TRUNC('week', reading_ts), device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(humidity_dense_rank) OVER w_part AS running_min_humidity_dense_rank,
        COUNT(humidity_dense_rank) OVER w_part AS running_count_humidity_dense_rank,
        AVG(humidity_dense_rank) OVER w_part AS running_avg_humidity_dense_rank,
        SUM(humidity_dense_rank) OVER w_part AS running_sum_humidity_dense_rank,
        SUM(humidity) OVER w_ordered AS cumulative_humidity_2
    FROM scored
    WINDOW w_part AS (PARTITION BY device_code ORDER BY humidity_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY MONTH(reading_ts), device_code ORDER BY humidity_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY cumulative_humidity_2 DESC, id) AS dedup_rank
        FROM with_windows
    ) ranked
    WHERE dedup_rank = 1
),
scored_device_readings AS (
    -- Running and moving aggregates
    SELECT
        id,
        humidity,
        firmware,
        last_known_humidity,
        humidity_dense_rank,
        running_min_humidity_dense_rank,
        running_count_humidity_dense_rank,
        running_avg_humidity_dense_rank,
        running_sum_humidity_dense_rank,
        dedup_rank,
        SUM(humidity) OVER w_recent AS cumulative_humidity_3,
        FIRST_VALUE(cumulative_humidity_2) IGNORE NULLS OVER w_recent AS first_known_cumulative_humidity_2,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS humidity_avg_3
    FROM latest
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY running_avg_humidity_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        id,
        humidity,
        firmware,
        last_known_humidity,
        running_count_humidity_dense_rank,
        running_avg_humidity_dense_rank,
        running_sum_humidity_dense_rank,
        dedup_rank,
        cumulative_humidity_3,
        first_known_cumulative_humidity_2,
        humidity_avg_3,
        CAST(dedup_rank AS DOUBLE) AS dedup_rank_str
    FROM scored_device_readings
    WHERE running_avg_humidity_dense_rank <= 7.3
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        humidity,
        last_known_humidity,
        running_count_humidity_dense_rank,
        running_avg_humidity_dense_rank,
        running_sum_humidity_dense_rank,
        dedup_rank,
        cumulative_humidity_3,
        first_known_cumulative_humidity_2,
        humidity_avg_3,
        dedup_rank_str,
        COALESCE(humidity, 0) AS humidity_filled
    FROM base_device_readings
    WHERE dedup_rank <= 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN humidity >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END AS humidity_tier,
    CASE WHEN first_known_cumulative_humidity_2 >= 336 THEN 'HIGH' ELSE 'LOW' END AS first_known_cumulative_humidity_2_tier,
    ROUND(AVG(humidity_filled), 2) AS avg_humidity_filled,
    PERCENTILE_APPROX(last_known_humidity, ARRAY(0.25, 0.5, 0.75)) AS last_known_humidity_quartiles
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN humidity >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN first_known_cumulative_humidity_2 >= 336 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN humidity >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY humidity_tier DESC
