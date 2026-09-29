WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
)
SELECT site_id, read_at
FROM recent_readings
WHERE voltage > 216.8
  AND quality_slug IS NOT NULL
UNION ALL
SELECT id, valid_from
FROM tariff_windows
