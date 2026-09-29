WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        INITCAP(TRIM(firmware)) AS firmware_title
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND device_code <> 'DV-01'
)
-- QUALIFY-style window filter
SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS row_num
FROM filtered
QUALIFY row_num = 1;
