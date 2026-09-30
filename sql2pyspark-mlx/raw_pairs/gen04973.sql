SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN status_bits >= 13 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    MAX(reading_ts) AS last_reading_ts,
    CORR(pressure, status_bits) AS corr_pressure_status_bits,
    APPROX_PERCENTILE(humidity, 0.9) AS humidity_p90
FROM device_readings
GROUP BY DATE_TRUNC('DAY', reading_ts), CASE WHEN status_bits >= 13 THEN 'HIGH' ELSE 'LOW' END
