WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        humidity * status_bits AS humidity_times_status_bits
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
      AND humidity BETWEEN 36 AND 69
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_pressure,
        COUNT(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_pressure,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_pressure
    FROM cleaned
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        reading_ts,
        humidity,
        last3_count_pressure,
        last3_avg_pressure,
        MAX(status_bits) OVER (PARTITION BY device_code ORDER BY last3_count_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_bits,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY last3_count_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_status_bits,
        MIN(status_bits) OVER (PARTITION BY device_code ORDER BY last3_count_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_bits
    FROM enriched
    WHERE humidity < 37
),
enriched_device_readings_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        last3_count_pressure,
        last3_avg_pressure,
        running_max_status_bits,
        running_sum_status_bits,
        running_min_status_bits,
        FIRST_VALUE(last3_avg_pressure) IGNORE NULLS OVER (ORDER BY running_min_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_avg_pressure,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(reading_ts) ORDER BY running_max_status_bits NULLS LAST) AS running_max_status_bits_dense_rank
    FROM enriched_device_readings
),
enriched_device_readings_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_min_status_bits) OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_min_status_bits,
        DENSE_RANK() OVER (ORDER BY running_min_status_bits DESC NULLS LAST) AS running_min_status_bits_dense_rank
    FROM enriched_device_readings_2
),
enriched_device_readings_4 AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(last3_avg_pressure) OVER (ORDER BY cumulative_running_min_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last3_avg_pressure,
        COUNT(last3_avg_pressure) OVER (ORDER BY cumulative_running_min_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last3_avg_pressure,
        MAX(last3_avg_pressure) OVER (ORDER BY cumulative_running_min_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last3_avg_pressure,
        LAG(cumulative_running_min_status_bits, 1) OVER (ORDER BY running_min_status_bits, id) AS prev_cumulative_running_min_status_bits
    FROM enriched_device_readings_3
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN last3_count_pressure >= 15 THEN 'LARGE' ELSE 'SMALL' END AS last3_count_pressure_tier,
        CASE WHEN humidity >= 74 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
        APPROX_PERCENTILE(first_known_last3_avg_pressure, ARRAY(0.25, 0.5, 0.75)) AS first_known_last3_avg_pressure_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM enriched_device_readings_4
    GROUP BY GROUPING SETS ((CASE WHEN last3_count_pressure >= 15 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN humidity >= 74 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN last3_count_pressure >= 15 THEN 'LARGE' ELSE 'SMALL' END), ())
    HAVING distinct_ids_2 > 1
)
SELECT
    last3_count_pressure_tier,
    humidity_tier,
    first_known_last3_avg_pressure_quartiles,
    distinct_ids
FROM totals
ORDER BY last3_count_pressure_tier DESC, humidity_tier
