WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        humidity - temp_c humidity_minus_temp_c,
        ROUND(pressure, 0) pressure_rounded
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND temp_c < 31
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
matched_keys AS (
    -- Set intersection on keys
    SELECT firmware
    FROM islands
    INTERSECT ALL
    SELECT firmware
    FROM device_readings
    WHERE humidity < 43
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN matched_keys k ON s.firmware = k.firmware
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        CAST(reading_ts AS DATE) reading_ts_day
    FROM reconciled
    WHERE firmware IN ('2.0.1', '1.1.0')
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(temp_c, 1) temp_c_rounded,
        CONCAT_WS('-', firmware, device_code) firmware_device_code_key
    FROM valid
    WHERE reading_ts_day < '2025-05-20'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, STDDEV_SAMP(pressure) OVER (PARTITION BY TO_DATE(reading_ts), device_code) reading_ts_to_date_stddev_samp_pressure
    FROM filtered_device_readings
    WHERE temp_c_rounded BETWEEN 47 AND 76
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('YEAR', reading_ts), device_code ORDER BY temp_c_rounded DESC NULLS LAST) temp_c_rounded_cume_dist,
        MAX(pressure) OVER (PARTITION BY device_code) device_code_max_pressure,
        CORR(pressure, temp_c) OVER (PARTITION BY device_code) corr_pressure_temp_c
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    device_code,
    metric_name,
    val
FROM (
    SELECT
        device_code,
        pressure,
        temp_c
    FROM metrics
) src
UNPIVOT (
    val FOR metric_name IN (pressure, temp_c)
);
