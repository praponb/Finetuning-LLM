WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        status_bits + humidity AS status_bits_plus_humidity,
        ROUND(temp_c / pressure, 2) AS temp_c_per_pressure
    FROM device_readings
    WHERE firmware != '1.1.0'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        pressure,
        temp_c
    FROM cleaned
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (pressure, temp_c)
) unp;
