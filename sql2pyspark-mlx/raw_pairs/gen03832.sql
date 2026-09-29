WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(pressure AS INT) AS pressure_int,
        DATE_TRUNC('week', reading_ts) AS reading_ts_period
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(temp_c, 2) AS temp_c_rounded
    FROM filtered
    WHERE pressure_int > 65
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessions_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) = 1
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM latest
),
sessions_device_readings_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, firmware
    FROM sessions_device_readings_2
    INTERSECT
    SELECT id, firmware
    FROM device_readings
    WHERE status_bits = 11
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_device_readings_2 s
    INNER JOIN matched_keys k ON s.id = k.id AND s.firmware = k.firmware
)
SELECT
    id,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    pressure_int,
    is_new_burst
FROM reconciled
ORDER BY pressure_int DESC NULLS LAST
