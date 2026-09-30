SELECT
    device_code,
    CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    BIT_OR(status_bits) AS status_bits_bit_or,
    MIN(reading_ts) AS started_at,
    APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles
FROM device_readings
GROUP BY ROLLUP (device_code, CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', reading_ts))
ORDER BY status_bits_tier
