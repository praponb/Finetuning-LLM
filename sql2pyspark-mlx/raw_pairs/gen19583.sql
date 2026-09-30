SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN status_bits >= 12 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_tier,
    PERCENTILE_APPROX(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    APPROX_PERCENTILE(status_bits, 0.5) AS status_bits_p50,
    GROUPING_ID() AS grouping_id
FROM device_readings
GROUP BY CUBE (DATE_TRUNC('DAY', reading_ts), CASE WHEN status_bits >= 12 THEN 'PREMIUM' ELSE 'STANDARD' END)
