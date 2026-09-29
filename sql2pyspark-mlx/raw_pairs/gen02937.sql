WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(temp_c, 0) temp_c_filled
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    device_code,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events,
    COUNT(*) AS row_count,
    MIN(reading_ts) AS first_reading_ts,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    SUM(CASE WHEN device_code = 'DV-02' THEN humidity ELSE 0 END) AS dv_02_humidity
FROM sessions_device_readings
GROUP BY device_code
