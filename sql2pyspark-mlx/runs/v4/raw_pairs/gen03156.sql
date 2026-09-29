WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(status_bits, 0) AS status_bits_rounded
    FROM device_readings
    WHERE pressure < 1005
)
SELECT firmware, device_code
FROM valid
EXCEPT DISTINCT
SELECT firmware, device_code
FROM device_readings
WHERE firmware = '2.0.1';
