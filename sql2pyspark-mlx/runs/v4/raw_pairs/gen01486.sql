WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, status_bits - temp_c AS status_bits_minus_temp_c
    FROM device_readings
    WHERE temp_c <= 33
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        pressure,
        humidity,
        temp_c,
        status_bits_minus_temp_c
    FROM valid
) src
UNPIVOT (
    metric_value FOR measure IN (pressure, humidity, temp_c, status_bits_minus_temp_c)
);
