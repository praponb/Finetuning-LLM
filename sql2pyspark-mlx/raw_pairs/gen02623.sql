WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        CAST(pressure AS INT) AS pressure_int
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
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
        pressure,
        temp_c
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (pressure, temp_c)
);
