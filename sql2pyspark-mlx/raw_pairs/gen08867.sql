WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(humidity, status_bits) AS least_humidity_status_bits,
        CASE WHEN temp_c >= 36.6 THEN 'High' WHEN temp_c >= 20.8 THEN 'Medium' ELSE 'Low' END AS temp_c_band
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
)
-- Running and moving aggregates
SELECT
    id,
    device_code,
    temp_c,
    humidity,
    firmware,
    least_humidity_status_bits,
    CORR(temp_c, humidity) OVER (PARTITION BY device_code) AS corr_temp_c_humidity,
    ROUND(pressure / SUM(pressure) OVER (PARTITION BY TO_DATE(reading_ts), device_code), 4) AS pressure_share
FROM valid
