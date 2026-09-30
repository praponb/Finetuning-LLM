WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, device_code
    FROM device_readings
    EXCEPT ALL
    SELECT id, device_code
    FROM device_readings
    WHERE device_code = 'DV-03'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN kept_keys k ON s.id = k.id AND s.device_code = k.device_code
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(firmware) AS firmware_lower,
        FLOOR(temp_c) AS temp_c_whole
    FROM reconciled
    WHERE humidity < 62
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(pressure, temp_c) OVER (PARTITION BY device_code) AS corr_pressure_temp_c,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS temp_c_avg_15m,
        CORR(status_bits, humidity) OVER (PARTITION BY device_code) AS corr_status_bits_humidity,
        MAX(temp_c_whole) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_whole_max_1h
    FROM valid
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY corr_status_bits_humidity NULLS FIRST) AS corr_status_bits_humidity_cume_dist,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY temp_c_whole_max_1h NULLS FIRST) AS temp_c_whole_max_1h_dense_rank,
        STDDEV_SAMP(temp_c_whole_max_1h) OVER (PARTITION BY device_code) AS device_code_stddev_samp_temp_c_whole_max_1h
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        device_code,
        humidity,
        pressure,
        corr_pressure_temp_c,
        corr_status_bits_humidity
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (humidity, pressure, corr_pressure_temp_c, corr_status_bits_humidity)
)
