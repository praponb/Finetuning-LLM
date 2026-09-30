SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    SUM(CASE WHEN device_code = 'DV-02' THEN humidity ELSE 0 END) AS dv_02_humidity,
    SUM(status_bits) FILTER (WHERE device_code = 'DV-04') AS dv_04_total_status_bits,
    COUNT(*) AS cnt,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows
FROM device_readings
GROUP BY CUBE (DATE_TRUNC('HOUR', reading_ts))
ORDER BY dv_04_rows DESC NULLS FIRST;
