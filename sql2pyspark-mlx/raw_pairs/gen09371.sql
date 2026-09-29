WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(humidity AS INT) AS humidity_int
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
      AND humidity >= 68
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(pressure) OVER w_grp AS running_count_pressure,
        SUM(pressure) OVER w_grp AS running_sum_pressure,
        AVG(pressure) OVER w_grp AS running_avg_pressure,
        MAX(pressure) OVER w_grp AS running_max_pressure,
        SUM(humidity_int) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity_int
    FROM base
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY humidity_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base_device_readings AS (
    -- Basic cleansing and defaults
    SELECT *, running_sum_pressure + pressure AS running_sum_pressure_plus_pressure
    FROM scored
    WHERE status_bits BETWEEN 4 AND 8
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM base_device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        running_count_pressure,
        running_avg_pressure,
        cumulative_humidity_int,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        running_avg_pressure,
        cumulative_humidity_int,
        burst_id,
        NTILE(10) OVER w_ordered AS humidity_bucket,
        NTILE(10) OVER w_running AS running_avg_pressure_bucket,
        LAG(running_avg_pressure, 2, 0) OVER (PARTITION BY device_code ORDER BY running_count_pressure, id) AS prev_running_avg_pressure,
        SUM(humidity) OVER w_running_2 AS running_humidity
    FROM islands
    WHERE burst_id BETWEEN 3 AND 4
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY humidity, id), w_running AS (PARTITION BY device_code ORDER BY running_avg_pressure, id), w_running_2 AS (PARTITION BY device_code ORDER BY running_count_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    burst_id,
    CASE WHEN prev_running_avg_pressure >= 1028 THEN 'LARGE' ELSE 'SMALL' END AS prev_running_avg_pressure_tier,
    ROUND(AVG(humidity_bucket), 2) AS avg_humidity_bucket,
    MIN(reading_ts) AS first_reading_ts,
    ROUND(AVG(burst_id), 2) AS avg_burst_id
FROM enriched
GROUP BY 1, 2, 3
