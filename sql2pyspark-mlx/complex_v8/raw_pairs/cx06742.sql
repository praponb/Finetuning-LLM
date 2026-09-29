WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
    WHERE kwh < 82.9
      AND site_id IS NOT NULL
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM cleaned_readings s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND LOWER(si.site_name) LIKE '%de%'
        )
)
SELECT *
FROM (
    SELECT read_at, quality, kwh
    FROM screened
) src
PIVOT (
    COUNT(kwh)
    FOR quality IN ('OK', 'BAD')
)
