WITH valid AS (
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
        CASE WHEN temp_c >= 34.7 THEN 'High' WHEN temp_c >= 26.6 THEN 'Medium' ELSE 'Low' END AS temp_c_level
    FROM device_readings
    WHERE reading_ts >= '2023-08-01'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure NULLS LAST, id) AS rn
        FROM sessionized
    ) ranked
    WHERE rn = 1
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
        is_new_session,
        rn,
        CAST(rn AS DOUBLE) AS rn_num,
        ROUND(temp_c / pressure, 2) AS temp_c_per_pressure
    FROM latest
    WHERE TO_DATE(reading_ts) < '2025-03-24'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_bits >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_tier,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
    CORR(humidity, temp_c) AS corr_humidity_temp_c,
    COUNT(*) AS num_rows,
    CORR(pressure, temp_c) AS corr_pressure_temp_c,
    MIN(reading_ts) AS started_at
FROM base
GROUP BY CASE WHEN status_bits >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END;
