WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        status_bits + humidity AS status_bits_plus_humidity
    FROM device_readings
    WHERE temp_c >= 33
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(temp_c, 2) AS temp_c_rounded
    FROM base
    WHERE reading_ts >= '2024-02-15'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        status_bits_plus_humidity,
        temp_c_rounded,
        CASE WHEN status_bits_plus_humidity >= 69 THEN 'High' WHEN status_bits_plus_humidity >= 18 THEN 'Medium' ELSE 'Low' END AS status_bits_plus_humidity_level
    FROM valid
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND pressure > 998
),
valid_device_readings AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(firmware) AS clean_firmware
    FROM cleaned
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 02:00:00')
      AND humidity > 64
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    PERCENTILE_APPROX(status_bits_plus_humidity, 0.9) AS status_bits_plus_humidity_p90,
    MIN(temp_c_rounded) AS min_temp_c_rounded,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    MIN(reading_ts) AS first_seen
FROM valid_device_readings
GROUP BY hour_window
HAVING ROUND(AVG(temp_c), 2) >= 0
ORDER BY min_temp_c_rounded DESC NULLS LAST
