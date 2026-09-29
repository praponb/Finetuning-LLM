WITH valid AS (
    SELECT *, ROUND(status_bits, 0) AS status_bits_rounded
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
)
SELECT id, device_code
FROM valid
EXCEPT DISTINCT
SELECT id, device_code
FROM device_readings
WHERE humidity > 44;
