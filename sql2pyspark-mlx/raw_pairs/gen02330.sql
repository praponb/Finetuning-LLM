WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(humidity, 0) AS humidity_rounded,
        pressure - status_bits AS pressure_minus_status_bits
    FROM device_readings
    WHERE pressure < 1019
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(humidity) OVER w_running AS humidity_avg_3,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('week', reading_ts), device_code ORDER BY pressure_minus_status_bits NULLS FIRST) AS pressure_minus_status_bits_percent_rank,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY humidity_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_bits_avg_3
    FROM filtered
    WHERE humidity_rounded BETWEEN 44 AND 67
    WINDOW w_running AS (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
filtered_device_readings AS (
    -- Keep recent, valid rows
    SELECT *, temp_c - humidity_avg_3 AS temp_c_minus_humidity_avg_3
    FROM with_windows
    WHERE temp_c <= 32
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM filtered_device_readings
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        humidity_rounded,
        humidity_avg_3,
        status_bits_avg_3,
        temp_c_minus_humidity_avg_3,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN status_bits_avg_3 >= 12 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_avg_3_tier,
        island_id,
        COUNT(DISTINCT device_code) AS distinct_device_codes,
        AVG(humidity) FILTER (WHERE device_code = 'DV-01') AS dv_01_avg_humidity,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN status_bits_avg_3 >= 12 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY AVG(humidity) FILTER (WHERE device_code = 'DV-01') DESC) AS dv_01_avg_humidity_percent_rank
    FROM sessions_device_readings
    GROUP BY CASE WHEN status_bits_avg_3 >= 12 THEN 'PREMIUM' ELSE 'STANDARD' END, island_id
    HAVING COUNT(DISTINCT device_code) > 1
)
SELECT
    status_bits_avg_3_tier,
    island_id,
    distinct_device_codes,
    dv_01_avg_humidity_percent_rank
FROM grouped
ORDER BY status_bits_avg_3_tier DESC
