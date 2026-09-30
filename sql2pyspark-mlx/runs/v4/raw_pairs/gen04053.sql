SELECT
    device_code,
    APPROX_PERCENTILE(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
    SUM(CASE WHEN device_code = 'DV-01' THEN status_bits ELSE 0 END) AS dv_01_status_bits
FROM device_readings
GROUP BY device_code;
