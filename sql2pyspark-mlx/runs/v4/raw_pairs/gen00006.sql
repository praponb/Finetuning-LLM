WITH matched_keys AS (
    SELECT device_code, firmware
    FROM device_readings
    EXCEPT DISTINCT
    SELECT device_code, firmware
    FROM device_readings
    WHERE reading_ts >= '2023-04-01'
),
verified AS (
    SELECT s.*
    FROM device_readings s
    INNER JOIN matched_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
)
SELECT
    CASE WHEN humidity >= 82 THEN 'LARGE' ELSE 'SMALL' END AS humidity_tier,
    PERCENTILE_APPROX(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    PERCENTILE_APPROX(status_bits, 0.5) AS status_bits_p50
FROM verified
GROUP BY CASE WHEN humidity >= 82 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_device_codes DESC NULLS FIRST;
