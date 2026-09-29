SELECT
    CASE WHEN humidity >= 81 THEN 'LARGE' ELSE 'SMALL' END AS humidity_tier,
    PERCENTILE_APPROX(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
    MIN(reading_ts) AS first_seen,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes_2
FROM device_readings
GROUP BY CASE WHEN humidity >= 81 THEN 'LARGE' ELSE 'SMALL' END
