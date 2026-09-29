WITH flagged AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
valid AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        is_new_session,
        session_no,
        ROUND(session_no / pressure, 2) AS session_no_per_pressure
    FROM sessionized
    WHERE reading_ts > DATE '2025-09-01'
),
valid_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        session_no,
        CAST(session_no_per_pressure AS INT) AS session_no_per_pressure_int,
        TRIM(firmware) AS clean_firmware
    FROM valid
    WHERE reading_ts >= TIMESTAMP '2025-09-01 06:00:00'
),
base AS (
    SELECT *, TRIM(device_code) AS clean_device_code
    FROM valid_device_readings
    WHERE humidity <= 42
),
ranked AS (
    SELECT
        id,
        temp_c,
        session_no,
        clean_device_code
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    SELECT
        *,
        CASE WHEN temp_c >= 41.5 THEN 'High' WHEN temp_c >= 24 THEN 'Medium' ELSE 'Low' END AS temp_c_band,
        CASE WHEN session_no >= 6 THEN 'High' WHEN session_no >= 4 THEN 'Medium' ELSE 'Low' END AS session_no_band
    FROM ranked
    WHERE session_no < 7
)
SELECT
    id,
    temp_c,
    session_no,
    clean_device_code,
    temp_c_band
FROM cleaned
ORDER BY session_no NULLS FIRST, id DESC
LIMIT 20;
