WITH base AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE voltage > 215.7
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM base
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM scored s
    LEFT JOIN sites si ON si.id = s.site_id
),
site_zone_stats AS (
    -- Median site_installed_kw per site_zone
    SELECT site_zone, PERCENTILE_APPROX(site_installed_kw, 0.5) AS median_site_installed_kw
    FROM enriched_site
    GROUP BY site_zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_site_installed_kw,
        ROUND(s.site_installed_kw - g.median_site_installed_kw, 2) AS site_installed_kw_minus_median
    FROM enriched_site s
    JOIN site_zone_stats g ON g.site_zone = s.site_zone
    WHERE s.site_installed_kw >= g.median_site_installed_kw
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_ref,
        s.read_at_quarter,
        s.quality_or_unknown,
        s.site_installed_kw,
        s.site_commissioned_on,
        s.site_zone,
        s.site_installed_kw_minus_median,
        si.site_name
    FROM outliers s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality_or_unknown,
    site_installed_kw,
    site_commissioned_on,
    site_zone,
    site_installed_kw_minus_median,
    site_name,
    SUBSTRING(TRIM(quality_ref), 1, 3) AS quality_ref_prefix,
    CONCAT(site_name, '#', CAST(site_id AS STRING)) AS site_name_ref
FROM joined_site
WHERE site_zone <> 'Z4'
ORDER BY site_installed_kw_minus_median DESC
