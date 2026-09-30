WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'EST')
      AND kwh BETWEEN 37.1 AND 81.9
)
SELECT id
FROM cleaned_readings
WHERE site_id <= 5
EXCEPT ALL
SELECT id
FROM meter_readings
WHERE quality <> 'BAD'
ORDER BY id DESC
