WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, HOUR(reading_ts) AS reading_ts_hour
    FROM device_readings
    WHERE temp_c < 23
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    device_code,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    BIT_AND(status_bits) AS status_bits_bit_and,
    SUM(humidity) FILTER (WHERE device_code = 'DV-04') AS dv_04_total_humidity,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', reading_ts) ORDER BY (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 DESC) AS span_minutes_dense_rank
FROM sessions_device_readings
GROUP BY DATE_TRUNC('HOUR', reading_ts), device_code
ORDER BY device_code ASC;
