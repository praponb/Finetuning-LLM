WITH cleaned AS (
    SELECT *, status_bits + pressure AS status_bits_plus_pressure
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 09:00:00'
      AND humidity IS NULL
)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        status_bits_plus_pressure,
        temp_c
    FROM cleaned
) src
UNPIVOT (
    value FOR kpi IN (status_bits_plus_pressure AS STATUS_BITS_PLUS_PRESSURE, temp_c AS TEMP_C)
) unp;
