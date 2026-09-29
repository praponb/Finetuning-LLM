WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(device_code, ', ', firmware) AS device_code_firmware
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
base AS (
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
        CAST(pressure AS INT) AS pressure_int,
        CASE WHEN humidity >= 65 THEN 'High' WHEN humidity >= 45 THEN 'Medium' ELSE 'Low' END AS humidity_tier
    FROM valid
    WHERE reading_ts > TIMESTAMP '2025-09-01 06:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        firmware,
        pressure_int,
        humidity_tier,
        CORR(status_bits, pressure) OVER (PARTITION BY device_code) AS corr_status_bits_pressure,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', reading_ts), device_code ORDER BY pressure, id) AS event_seq,
        SUM(pressure_int) OVER (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure_int
    FROM base
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_pressure_int) OVER (PARTITION BY TO_DATE(reading_ts) ORDER BY running_pressure_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_pressure_int,
        NTILE(5) OVER (ORDER BY running_pressure_int, id) AS running_pressure_int_bucket
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    kpi,
    value
FROM (
    SELECT
        id,
        corr_status_bits_pressure,
        humidity,
        pressure
    FROM enriched_device_readings
) src
UNPIVOT (
    value FOR kpi IN (corr_status_bits_pressure, humidity, pressure)
)
