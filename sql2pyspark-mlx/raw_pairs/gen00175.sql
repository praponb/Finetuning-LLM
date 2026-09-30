WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(status_bits / pressure, 2) status_bits_per_pressure,
        GREATEST(temp_c, humidity) greatest_temp_c_humidity
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 01:00:00'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        status_bits_per_pressure,
        greatest_temp_c_humidity,
        LEAST(temp_c, status_bits) least_temp_c_status_bits
    FROM base
    WHERE reading_ts > DATE '2025-09-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        status_bits_per_pressure,
        greatest_temp_c_humidity,
        least_temp_c_status_bits,
        CAST(status_bits AS DOUBLE) status_bits_str
    FROM valid
    WHERE status_bits >= 4
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        humidity,
        status_bits,
        status_bits_per_pressure,
        greatest_temp_c_humidity,
        least_temp_c_status_bits,
        status_bits_str
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_per_pressure, id) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_bits,
        greatest_temp_c_humidity,
        least_temp_c_status_bits,
        ROUND(greatest_temp_c_humidity, 1) greatest_temp_c_humidity_rounded
    FROM ranked
    WHERE humidity BETWEEN 53 AND 63
),
valid_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_bits,
        greatest_temp_c_humidity,
        least_temp_c_status_bits,
        greatest_temp_c_humidity_rounded,
        greatest_temp_c_humidity * least_temp_c_status_bits greatest_temp_c_humidity_times_least_temp_c_status_bits
    FROM filtered_device_readings
    WHERE greatest_temp_c_humidity BETWEEN 45 AND 63
)
-- Grouped metrics
SELECT
    CASE WHEN greatest_temp_c_humidity_times_least_temp_c_status_bits >= 87 THEN 'LARGE' ELSE 'SMALL' END AS greatest_temp_c_humidity_times_least_temp_c_status_bits_tier,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(least_temp_c_status_bits, ARRAY(0.25, 0.5, 0.75)) AS least_temp_c_status_bits_quartiles,
    SUM(greatest_temp_c_humidity_times_least_temp_c_status_bits) AS total_greatest_temp_c_humidity_times_least_temp_c_status_bits,
    BIT_OR(status_bits) AS status_bits_bit_or
FROM valid_device_readings
GROUP BY CASE WHEN greatest_temp_c_humidity_times_least_temp_c_status_bits >= 87 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY num_rows DESC
