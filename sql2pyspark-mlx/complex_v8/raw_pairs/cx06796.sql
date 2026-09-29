WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(quality_slug)) AS quality_slug_len,
        LENGTH(TRIM(quality_slug)) AS quality_slug_len_2,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM recent_readings
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality_slug,
    quality_slug_len
FROM without_matches
WHERE quality_slug_len <= 12
ORDER BY id DESC
