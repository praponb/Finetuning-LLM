WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage BETWEEN 220.7 AND 239.6
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        si.site_name
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, read_at, quality, kwh
    FROM combined_details_site
) src
PIVOT (
    COUNT(kwh)
    FOR quality IN ('BAD', 'EST', 'OK')
)
ORDER BY site_id, read_at
