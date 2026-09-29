WITH cleaned AS (
    SELECT *, CAST(humidity AS INT) AS humidity_int
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 07:00:00'
)
SELECT
    id,
    device_code,
    measure,
    value
FROM (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure
    FROM cleaned
) src
UNPIVOT (
    value FOR measure IN (temp_c AS TEMP_C, humidity AS HUMIDITY, pressure AS PRESSURE)
);
