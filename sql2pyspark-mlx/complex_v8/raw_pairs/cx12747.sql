WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
summary AS (
    -- Group-level totals
    SELECT
        quality,
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        site_tariff,
        COUNT(*) AS cnt,
        MIN(site_commissioned_on) AS first_site_commissioned_on,
        ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list
    FROM with_details_site
    GROUP BY quality, DATE_TRUNC('MONTH', site_commissioned_on), site_tariff
)
-- Final projection
SELECT
    quality,
    site_tariff,
    cnt,
    first_site_commissioned_on,
    site_tariff_list,
    COALESCE(cnt, 0) AS cnt_filled
FROM summary
ORDER BY quality
