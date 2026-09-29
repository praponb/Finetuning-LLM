SELECT *
FROM (
    SELECT
        id,
        device_code,
        pressure,
        humidity
    FROM device_readings
) src
UNPIVOT (
    metric_value FOR measure IN (pressure, humidity AS HUMIDITY)
)
