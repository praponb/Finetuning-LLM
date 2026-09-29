WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        firmware,
        humidity - temp_c AS humidity_minus_temp_c
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 14:00:00'
      AND pressure BETWEEN 992 AND 1013
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(humidity_minus_temp_c, 1) AS humidity_minus_temp_c_rounded
    FROM ranked
    WHERE firmware != '1.1.0'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN humidity_minus_temp_c >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS humidity_minus_temp_c_tier,
    BIT_OR(status_bits) AS status_bits_bit_or,
    AVG(humidity_minus_temp_c) AS avg_humidity_minus_temp_c,
    COUNT(*) AS num_rows
FROM filtered
GROUP BY CASE WHEN humidity_minus_temp_c >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY num_rows DESC NULLS FIRST;
