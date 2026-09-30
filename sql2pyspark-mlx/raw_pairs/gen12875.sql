WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(reading_ts AS DATE) AS reading_ts_day
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND pressure <= 1002
)
SELECT firmware
FROM base
INTERSECT
SELECT firmware
FROM device_readings
WHERE firmware = '1.0.3';
