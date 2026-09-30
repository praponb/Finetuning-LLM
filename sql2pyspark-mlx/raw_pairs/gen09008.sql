SELECT
    CASE WHEN pressure >= 1019 THEN 'LARGE' ELSE 'SMALL' END AS pressure_tier,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count,
    ROUND(SUM(humidity) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS humidity_per_dv_01,
    AVG(humidity) FILTER (WHERE device_code = 'DV-04') AS dv_04_avg_humidity,
    PERCENTILE_APPROX(status_bits, ARRAY(0.25, 0.5, 0.75)) AS status_bits_quartiles
FROM device_readings
GROUP BY CUBE (CASE WHEN pressure >= 1019 THEN 'LARGE' ELSE 'SMALL' END);
