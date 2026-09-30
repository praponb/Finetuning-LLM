SELECT
    id,
    device_code,
    metric_name,
    val
FROM (
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        temp_c
    FROM device_readings
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric_name IN (humidity AS HUMIDITY, pressure AS PRESSURE, temp_c)
) unp
