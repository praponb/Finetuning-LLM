WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
)
-- Running sum of the flag = session number
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM gaps;
