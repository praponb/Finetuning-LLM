SELECT
    device_code,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    SUM(CASE WHEN device_code = 'DV-01' THEN pressure ELSE 0 END) AS dv_01_pressure,
    MAX(pressure) FILTER (WHERE device_code = 'DV-02') AS dv_02_max_pressure,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    COUNT(*) AS num_rows,
    STDDEV_SAMP(temp_c) AS temp_c_stddev
FROM device_readings
GROUP BY device_code
ORDER BY temp_c_stddev DESC NULLS LAST;
