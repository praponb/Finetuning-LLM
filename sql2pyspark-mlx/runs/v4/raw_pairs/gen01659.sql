WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(device_code) AS device_code_upper
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 03:00:00'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
deduped_device_readings AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY session_id DESC) AS rn
        FROM islands
    ) x
    WHERE rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        humidity,
        temp_c,
        pressure
    FROM deduped_device_readings
) src
UNPIVOT INCLUDE NULLS (
    val FOR kpi IN (humidity, temp_c AS TEMP_C, pressure)
);
