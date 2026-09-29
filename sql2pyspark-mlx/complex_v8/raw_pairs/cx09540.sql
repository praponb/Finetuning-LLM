WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
    WHERE voltage < 233.2
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_month,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        kwh,
        voltage,
        read_at_month,
        site_tariff,
        site_installed_kw,
        CONCAT(site_tariff, '#', CAST(id AS STRING)) AS site_tariff_ref,
        CASE WHEN kwh >= 78.6 THEN 'HIGH' WHEN kwh >= 37.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        LENGTH(TRIM(quality)) AS quality_len
    FROM with_parent_site
)
SELECT id, metric, value
FROM (
    SELECT id, site_installed_kw, voltage
    FROM annotated
) src
UNPIVOT (
    value FOR metric IN (site_installed_kw AS site_installed_kw_v, voltage AS voltage_v)
) unp
