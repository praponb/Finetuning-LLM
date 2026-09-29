WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        ROUND(status_bits / temp_c, 2) AS status_bits_per_temp_c,
        humidity - status_bits AS humidity_minus_status_bits
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, WEEKOFYEAR(reading_ts) AS reading_ts_weekofyear
    FROM base
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 02:00:00')
      AND status_bits_per_temp_c > 64
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        humidity_minus_status_bits,
        QUARTER(reading_ts) AS reading_ts_quarter,
        COALESCE(reading_ts_weekofyear, 0) AS reading_ts_weekofyear_filled
    FROM filtered
    WHERE reading_ts >= DATE '2025-09-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        reading_ts_weekofyear_filled,
        COUNT(reading_ts_weekofyear_filled) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS reading_ts_weekofyear_filled_count_1h,
        AVG(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_bits_avg_15m,
        COUNT(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pressure_count_2h
    FROM cleaned
    WHERE reading_ts_weekofyear_filled BETWEEN 35 AND 36
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(reading_ts_weekofyear_filled_count_1h / humidity, 2) AS reading_ts_weekofyear_filled_count_1h_per_humidity
    FROM enriched
    WHERE reading_ts_weekofyear_filled > 21
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        humidity,
        reading_ts_weekofyear_filled,
        reading_ts_weekofyear_filled_count_1h,
        status_bits_avg_15m,
        reading_ts_weekofyear_filled_count_1h * status_bits_avg_15m AS reading_ts_weekofyear_filled_count_1h_times_status_bits_avg_15m
    FROM filtered_device_readings
    WHERE pressure_count_2h <= 10
)
SELECT
    id,
    humidity,
    reading_ts_weekofyear_filled,
    reading_ts_weekofyear_filled_count_1h,
    status_bits_avg_15m,
    reading_ts_weekofyear_filled_count_1h_times_status_bits_avg_15m
FROM valid;
