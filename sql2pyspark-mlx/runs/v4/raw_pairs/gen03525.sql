WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        CAST(humidity AS INT) AS humidity_int
    FROM device_readings
    WHERE reading_ts >= '2025-05-15'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, status_bits + pressure AS status_bits_plus_pressure
    FROM valid
    WHERE reading_ts > TO_DATE('2025-09-01')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, humidity_int - pressure AS humidity_int_minus_pressure
    FROM cleaned
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN humidity_int_minus_pressure >= 71 THEN 'HIGH' ELSE 'LOW' END AS humidity_int_minus_pressure_tier,
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        MIN(reading_ts) AS first_reading_ts,
        COUNT_IF(device_code = 'DV-01') AS dv_01_events,
        SUM(humidity_int_minus_pressure) AS total_humidity_int_minus_pressure,
        STDDEV_SAMP(pressure) AS pressure_stddev,
        PERCENTILE_APPROX(humidity_int, 0.9) AS humidity_int_p90,
        RANK() OVER (PARTITION BY CASE WHEN humidity_int_minus_pressure >= 71 THEN 'HIGH' ELSE 'LOW' END ORDER BY PERCENTILE_APPROX(humidity_int, 0.9) DESC) AS humidity_int_p90_rank
    FROM filtered
    GROUP BY CASE WHEN humidity_int_minus_pressure >= 71 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', reading_ts)
)
SELECT
    humidity_int_minus_pressure_tier,
    hour_window,
    first_reading_ts,
    pressure_stddev
FROM stats
