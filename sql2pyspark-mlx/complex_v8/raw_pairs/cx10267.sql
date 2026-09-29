WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE (kwh > 46.6 OR site_id IS NOT NULL)
)
SELECT
    id,
    quality,
    quality_len,
    'reading' AS origin
FROM base_readings
WHERE TO_DATE(read_at) < DATE '2025-03-08'
UNION
SELECT
    id,
    quality,
    site_id AS quality_len,
    'reading_2' AS origin
FROM meter_readings
ORDER BY origin DESC, quality
