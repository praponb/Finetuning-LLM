WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS humidity_count_1h,
        FIRST_VALUE(temp_c, TRUE) OVER (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        LAG(humidity, 1) OVER (PARTITION BY DATE_TRUNC('WEEK', reading_ts) ORDER BY status_bits, id) AS prev_humidity,
        AVG(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_bits_avg_6
    FROM device_readings
)
-- Multi-level aggregation
SELECT
    device_code,
    CASE WHEN first_known_temp_c >= 41.2 THEN 'HIGH' ELSE 'LOW' END AS first_known_temp_c_tier,
    BIT_AND(status_bits) AS status_bits_bit_and,
    MIN(humidity) AS min_humidity,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(humidity_count_1h, 0.9) AS humidity_count_1h_p90,
    COUNT_IF(device_code = 'DV-04') AS dv_04_events,
    MIN(reading_ts) AS first_reading_ts
FROM scored
GROUP BY device_code, CASE WHEN first_known_temp_c >= 41.2 THEN 'HIGH' ELSE 'LOW' END
ORDER BY first_known_temp_c_tier DESC;
