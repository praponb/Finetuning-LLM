SELECT
    CASE WHEN temp_c >= 37.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    CASE WHEN humidity >= 74 THEN 'LARGE' ELSE 'SMALL' END AS humidity_tier,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    ROUND(SUM(pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) AS pressure_per_dv_04,
    ROUND(SUM(status_bits) / NULLIF(COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END), 0), 2) AS status_bits_per_dv_03,
    SUM(CASE WHEN device_code = 'DV-02' THEN humidity ELSE 0 END) AS dv_02_humidity,
    ROUND(AVG(status_bits), 2) AS avg_status_bits,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes_2
FROM device_readings
GROUP BY GROUPING SETS ((CASE WHEN temp_c >= 37.3 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN humidity >= 74 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN temp_c >= 37.3 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING ROUND(AVG(status_bits), 2) > 0
ORDER BY avg_status_bits NULLS LAST;
