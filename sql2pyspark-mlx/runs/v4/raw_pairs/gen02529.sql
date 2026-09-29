WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c * pressure AS temp_c_times_pressure
    FROM device_readings
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 03:00:00')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY temp_c_times_pressure) AS rn
    FROM valid
    QUALIFY rn = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(pressure) AS pressure_whole,
        ROUND(status_bits * 1.1, 2) AS status_bits_scaled
    FROM deduped
    WHERE humidity > 54
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_times_pressure,
        rn,
        pressure_whole,
        status_bits_scaled,
        new_group_flag,
        island_id,
        LAST_VALUE(humidity, TRUE) OVER w_part AS last_known_humidity,
        LEAD(island_id, 1) OVER (PARTITION BY device_code ORDER BY temp_c_times_pressure, id) AS next_island_id
    FROM sessions_device_readings
    WINDOW w_part AS (PARTITION BY device_code ORDER BY status_bits_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        temp_c_times_pressure
    FROM scored
) src
PIVOT (
    MAX(temp_c_times_pressure)
    FOR device_code IN ('DV-01' AS dv_01, 'DV-03' AS dv_03, 'DV-02' AS dv_02)
);
