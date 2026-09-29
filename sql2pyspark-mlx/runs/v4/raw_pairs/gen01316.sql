WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts
    FROM device_readings
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 02:00:00')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        days_since_reading_ts,
        ROUND(pressure / humidity, 2) AS pressure_per_humidity
    FROM filtered
    WHERE pressure < 1013
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure_per_humidity DESC NULLS FIRST, id) AS row_num
        FROM valid
    ) x
    WHERE row_num = 1
),
valid_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        row_num,
        status_bits + days_since_reading_ts AS status_bits_plus_days_since_reading_ts
    FROM latest
    WHERE firmware = '2.0.1'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, device_code
    FROM valid_device_readings
    INTERSECT ALL
    SELECT id, device_code
    FROM device_readings
    WHERE reading_ts >= '2025-02-05'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_device_readings s
    INNER JOIN kept_keys k ON s.id = k.id AND s.device_code = k.device_code
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        device_code,
        temp_c,
        status_bits_plus_days_since_reading_ts,
        humidity
    FROM kept_rows
) src
UNPIVOT (
    val FOR metric_name IN (temp_c, status_bits_plus_days_since_reading_ts AS STATUS_BITS_PLUS_DAYS_SINCE_READING_TS, humidity)
)
