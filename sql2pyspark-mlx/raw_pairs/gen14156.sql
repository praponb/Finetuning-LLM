WITH flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    session_no,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    PERCENTILE_APPROX(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
    MIN(reading_ts) AS first_seen,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count
FROM sessionized
GROUP BY hour_window, session_no;
