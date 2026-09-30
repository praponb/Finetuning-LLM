WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(voltage, 0.0) AS voltage_filled,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN kwh_filled >= 90.0 THEN 'A' WHEN kwh_filled >= 36.0 THEN 'B' ELSE 'C' END AS kwh_filled_tier
    FROM valid_readings
    WHERE voltage_filled >= 218.8
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM tagged
    WHERE MONTH(read_at) IN (2, 3)
),
latest AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_filled,
        kwh_filled,
        kwh_filled_tier,
        quality_group,
        quality_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage DESC, id DESC) AS rn
        FROM tagged_2
    ) ranked
    WHERE rn = 1
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.voltage_filled,
        s.kwh_filled,
        s.quality_clean,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM latest s
    JOIN sites si ON si.id = s.site_id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM combined_details_site
    WHERE site_installed_kw > (SELECT MIN(site_installed_kw) FROM combined_details_site)
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.voltage_filled,
        s.kwh_filled,
        s.quality_clean,
        s.site_installed_kw,
        s.site_commissioned_on,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM with_activity s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality,
    kwh_filled,
    site_installed_kw,
    site_commissioned_on,
    reading_id,
    reading_read_at,
    ABS(site_installed_kw - 284.0) AS site_installed_kw_distance
FROM with_children
WHERE reading_quality IN ('OK', 'BAD')
ORDER BY kwh_filled
