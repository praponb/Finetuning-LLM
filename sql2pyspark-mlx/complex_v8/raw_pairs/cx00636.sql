WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 3)
      AND quality = 'OK'
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.read_at_hour,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM prepared s
    LEFT JOIN sites si ON si.id = s.site_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        quality,
        read_at_hour,
        site_commissioned_on,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM enriched_site
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(quality_or_unknown), ' ', '_')) AS quality_or_unknown_slug,
        CASE WHEN quality_or_unknown IN ('EST', 'OK') THEN 'group_a' WHEN quality_or_unknown = 'UNKNOWN' THEN 'group_b' ELSE 'other' END AS quality_or_unknown_group,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM labeled
    WHERE quality IS NOT NULL
)
-- Final projection
SELECT
    id,
    quality,
    read_at_hour,
    site_commissioned_on,
    quality_or_unknown,
    quality_group
FROM derived
WHERE quality_or_unknown NOT IN ('EST', 'OK')
ORDER BY read_at_hour DESC NULLS FIRST, quality_group NULLS LAST, id
LIMIT 20
