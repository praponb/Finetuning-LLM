WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        firmware,
        INITCAP(TRIM(device_code)) device_code_title
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 13:00:00'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Grouped metrics
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    APPROX_PERCENTILE(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
    PERCENTILE_APPROX(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events,
    PERCENTILE_APPROX(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles_2,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events_2
FROM islands
GROUP BY session_start_flag_tier
ORDER BY dv_02_events DESC
