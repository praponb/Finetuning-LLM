WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage <= 236.7
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM prepared_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 241.8
              AND m.kwh > 34.1
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_slug,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM with_activity
),
deduped AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_slug,
        days_since_read_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_slug,
    days_since_read_at,
    SUBSTRING(TRIM(quality_slug), 1, 2) AS quality_slug_prefix
FROM deduped
ORDER BY days_since_read_at
