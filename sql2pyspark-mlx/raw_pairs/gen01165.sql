WITH gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    session_no,
    BIT_OR(status_bits) AS status_bits_bit_or,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
    AVG(humidity) AS avg_humidity,
    SUM(CASE WHEN device_code = 'DV-04' THEN session_no ELSE 0 END) AS dv_04_session_no,
    CORR(status_bits, pressure) AS corr_status_bits_pressure,
    AVG(session_no) AS avg_session_no
FROM sessions_device_readings
GROUP BY session_no
ORDER BY status_bits_bit_or;
