WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(voltage * 1.07, 2) AS voltage_adj,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
    WHERE quality <> 'OK'
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_prefix,
        s.read_at_month,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM cleaned_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Label and score rows
    SELECT *, QUARTER(site_commissioned_on) AS site_commissioned_on_quarter
    FROM enriched
    WHERE kwh <= 81.3
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality_prefix,
    read_at_month,
    site_installed_kw,
    site_commissioned_on_quarter
FROM tagged
ORDER BY read_at_month DESC NULLS FIRST
