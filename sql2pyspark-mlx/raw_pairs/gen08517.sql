WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN temp_c >= 40.7 THEN 'High' WHEN temp_c >= 20.5 THEN 'Medium' ELSE 'Low' END temp_c_band,
        pressure - humidity pressure_minus_humidity
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND firmware NOT IN ('1.0.3', '1.1.0')
)
-- Grouped metrics
SELECT
    CASE WHEN pressure_minus_humidity >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_minus_humidity_tier,
    ROUND(AVG(status_bits), 2) AS avg_status_bits,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
    AVG(humidity) AS avg_humidity
FROM filtered
GROUP BY CASE WHEN pressure_minus_humidity >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING AVG(humidity) > 0;
