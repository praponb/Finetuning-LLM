WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        LENGTH(TRIM(quality)) AS quality_len,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_len,
        s.read_at_month,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    site_id,
    site_tariff,
    quality,
    SUM(CASE WHEN site_tariff IN ('FLAT', 'EV') THEN read_at_month ELSE 0 END) AS read_at_month_site_tariff_in_flat_ev,
    ROUND(SUM(site_installed_kw) / NULLIF(SUM(voltage), 0), 3) AS site_installed_kw_to_voltage_ratio
FROM with_parent_site
GROUP BY GROUPING SETS ((site_id, site_tariff, quality), (site_id))
ORDER BY site_id NULLS LAST, site_tariff NULLS LAST, quality NULLS LAST
