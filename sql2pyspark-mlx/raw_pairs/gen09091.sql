WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        LEAST(pressure, temp_c) AS least_pressure_temp_c
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        least_pressure_temp_c,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(least_pressure_temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_least_pressure_temp_c,
        FIRST_VALUE(least_pressure_temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_least_pressure_temp_c_2
    FROM islands
    WHERE firmware = '1.1.0'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        first_known_least_pressure_temp_c,
        SUM(first_known_least_pressure_temp_c_2) OVER (PARTITION BY device_code ORDER BY least_pressure_temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_first_known_least_pressure_temp_c_2,
        AVG(first_known_least_pressure_temp_c_2) OVER (PARTITION BY device_code ORDER BY least_pressure_temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_first_known_least_pressure_temp_c_2,
        COUNT(first_known_least_pressure_temp_c_2) OVER (PARTITION BY device_code ORDER BY least_pressure_temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_first_known_least_pressure_temp_c_2,
        MAX(first_known_least_pressure_temp_c_2) OVER (PARTITION BY device_code ORDER BY least_pressure_temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_least_pressure_temp_c_2
    FROM metrics
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_max_first_known_least_pressure_temp_c_2 >= 68 THEN 'LARGE' ELSE 'SMALL' END AS running_max_first_known_least_pressure_temp_c_2_tier,
        CASE WHEN running_avg_first_known_least_pressure_temp_c_2 >= 86 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_first_known_least_pressure_temp_c_2_tier,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(running_sum_first_known_least_pressure_temp_c_2, 0.95) AS running_sum_first_known_least_pressure_temp_c_2_p95,
        PERCENTILE_APPROX(first_known_least_pressure_temp_c, ARRAY(0.25, 0.5, 0.75)) AS first_known_least_pressure_temp_c_quartiles
    FROM with_windows
    GROUP BY running_max_first_known_least_pressure_temp_c_2_tier, running_avg_first_known_least_pressure_temp_c_2_tier
)
SELECT
    running_max_first_known_least_pressure_temp_c_2_tier,
    running_avg_first_known_least_pressure_temp_c_2_tier,
    cnt,
    running_sum_first_known_least_pressure_temp_c_2_p95,
    first_known_least_pressure_temp_c_quartiles
FROM summary
ORDER BY running_avg_first_known_least_pressure_temp_c_2_tier ASC NULLS LAST, running_sum_first_known_least_pressure_temp_c_2_p95 ASC;
