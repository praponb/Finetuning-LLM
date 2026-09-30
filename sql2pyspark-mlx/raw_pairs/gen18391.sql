SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    AVG(pressure) FILTER (WHERE device_code = 'DV-04') AS dv_04_avg_pressure,
    COUNT(*) AS row_count,
    STDDEV_SAMP(pressure) AS pressure_stddev
FROM device_readings
GROUP BY DATE_TRUNC('HOUR', reading_ts)
ORDER BY hour_window ASC;
