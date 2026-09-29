WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(firmware) AS firmware_length
    FROM device_readings
    WHERE status_bits <> 10
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN temp_c >= 38.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    CORR(temp_c, status_bits) AS corr_temp_c_status_bits,
    SUM(CASE WHEN device_code = 'DV-03' THEN island_id ELSE 0 END) AS dv_03_island_id,
    SUM(CASE WHEN device_code = 'DV-01' THEN firmware_length ELSE 0 END) AS dv_01_firmware_length,
    AVG(temp_c) FILTER (WHERE device_code = 'DV-04') AS dv_04_avg_temp_c,
    GROUPING(CASE WHEN temp_c >= 38.6 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM islands
GROUP BY ROLLUP (CASE WHEN temp_c >= 38.6 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING dv_04_avg_temp_c >= 0
ORDER BY dv_01_firmware_length NULLS FIRST
