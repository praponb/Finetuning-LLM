WITH valid AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(status_bits * 1.07, 2) AS status_bits_adjusted
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
)
SELECT
    id,
    measure,
    reading
FROM (
    SELECT
        id,
        status_bits_adjusted,
        temp_c
    FROM valid
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (status_bits_adjusted, temp_c AS TEMP_C)
);
