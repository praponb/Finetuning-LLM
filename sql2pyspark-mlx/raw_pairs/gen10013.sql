WITH enriched AS (
    SELECT
        *,
        LAG(temp_c, 1) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_temp_c,
        NTILE(3) OVER w_hist AS status_bits_bucket
    FROM device_readings
    WHERE humidity < 36
    WINDOW w_hist AS (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY status_bits, id)
)
SELECT
    CASE WHEN temp_c >= 41.8 THEN 'HIGH' ELSE 'LOW' END AS temp_c_tier,
    MIN(status_bits_bucket) AS min_status_bits_bucket,
    MAX(pressure) AS max_pressure,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM enriched
GROUP BY temp_c_tier
ORDER BY max_pressure DESC
