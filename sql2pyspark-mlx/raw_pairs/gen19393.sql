WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(temp_c) temp_c_whole,
        humidity + pressure humidity_plus_pressure
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits - temp_c_whole status_bits_minus_temp_c_whole
    FROM filtered
    WHERE reading_ts > TIMESTAMP '2025-09-01 04:00:00'
      AND humidity_plus_pressure IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(pressure, status_bits) greatest_pressure_status_bits,
        COALESCE(pressure, 0) pressure_filled
    FROM cleaned
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
valid_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        firmware,
        status_bits_minus_temp_c_whole,
        greatest_pressure_status_bits,
        pressure_filled,
        pressure + status_bits_minus_temp_c_whole pressure_plus_status_bits_minus_temp_c_whole
    FROM valid
    WHERE humidity <= 69
),
valid_device_readings_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, temp_c * humidity temp_c_times_humidity
    FROM valid_device_readings
    WHERE temp_c BETWEEN 24 AND 33
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(status_bits_minus_temp_c_whole) OVER w_ordered running_min_status_bits_minus_temp_c_whole,
        MAX(status_bits_minus_temp_c_whole) OVER w_ordered running_max_status_bits_minus_temp_c_whole,
        COUNT(status_bits_minus_temp_c_whole) OVER w_ordered running_count_status_bits_minus_temp_c_whole,
        SUM(status_bits_minus_temp_c_whole) OVER w_ordered running_sum_status_bits_minus_temp_c_whole
    FROM valid_device_readings_2
    WHERE firmware = '1.0.3'
    WINDOW w_ordered AS (ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN humidity >= 77 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
    ROUND(AVG(pressure_plus_status_bits_minus_temp_c_whole), 2) AS avg_pressure_plus_status_bits_minus_temp_c_whole,
    AVG(running_count_status_bits_minus_temp_c_whole) AS avg_running_count_status_bits_minus_temp_c_whole,
    COUNT(DISTINCT firmware) AS distinct_firmwares
FROM metrics
GROUP BY 1
