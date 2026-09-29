SELECT *
FROM (
    SELECT
        id,
        device_code,
        humidity,
        pressure
    FROM device_readings
) src
UNPIVOT (
    value FOR measure IN (humidity AS HUMIDITY, pressure)
) unp
