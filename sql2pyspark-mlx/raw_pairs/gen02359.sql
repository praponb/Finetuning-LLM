WITH filtered AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), reading_ts) days_since_reading_ts,
        ROUND(status_bits, 2) status_bits_rounded
    FROM device_readings
    WHERE reading_ts >= '2024-05-27'
)
SELECT firmware, device_code
FROM filtered
EXCEPT ALL
SELECT firmware, device_code
FROM device_readings
WHERE HOUR(reading_ts) BETWEEN 6 AND 14
