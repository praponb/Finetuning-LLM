WITH valid AS (
    SELECT *, humidity - temp_c humidity_minus_temp_c
    FROM device_readings
    WHERE pressure > 1014
)
SELECT
    id,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_minus_temp_c DESC, id) AS rn
    FROM valid
) ranked
WHERE rn = 1;
