WITH flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    device_code,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    COUNT(*) AS cnt,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes_2,
    SUM(CASE WHEN device_code = 'DV-03' THEN session_no ELSE 0 END) AS dv_03_session_no,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows
FROM islands
GROUP BY device_code, activity_day
ORDER BY span_minutes_2 NULLS FIRST;
