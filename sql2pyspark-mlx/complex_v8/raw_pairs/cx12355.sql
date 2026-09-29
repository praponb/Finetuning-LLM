WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh <= 96.8
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_clean,
        si.site_name
    FROM cleaned_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
latest AS (
    -- Keep the earliest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_clean,
        site_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage, id) AS rn
        FROM combined_details_site
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        read_at + INTERVAL 1 DAYS AS read_at_plus_1d,
        CONCAT_WS(': ', site_name, quality) AS site_name_quality_label
    FROM latest
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality_clean,
    site_name,
    site_name_quality_label,
    CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
FROM labeled
ORDER BY id
