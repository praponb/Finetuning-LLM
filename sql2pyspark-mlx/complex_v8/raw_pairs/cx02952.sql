WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE quality IN ('EST', 'BAD')
      AND HOUR(read_at) BETWEEN 9 AND 14
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.zone AS site_zone
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        quality_prefix,
        quality_slug,
        site_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM enriched_site
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    quality_slug,
    MONTH(read_at) AS read_at_month
FROM deduped
ORDER BY quality_slug DESC, kwh DESC
