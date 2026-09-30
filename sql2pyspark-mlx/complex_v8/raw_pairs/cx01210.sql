WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref_2,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM meter_readings
    WHERE quality = 'BAD'
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_ref,
        s.quality_ref_2,
        s.read_at_day,
        si.site_name,
        si.tariff AS site_tariff
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
    WHERE UPPER(TRIM(si.site_name)) LIKE 'R%'
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    GROUPING_ID() AS gid,
    SUM(voltage) AS total_voltage,
    MAX(kwh) AS max_kwh,
    MAX(read_at_day) AS last_read_at_day
FROM enriched_site
GROUP BY GROUPING SETS ((quality, site_id, site_tariff), (quality), ())
ORDER BY quality NULLS LAST, site_id NULLS LAST, site_tariff NULLS LAST
