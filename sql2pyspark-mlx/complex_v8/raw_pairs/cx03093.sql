WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE kwh > 45.3
      AND HOUR(read_at) BETWEEN 7 AND 13
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.voltage_per_kwh,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id AND si.tariff IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        kwh + voltage AS kwh_plus_voltage,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM with_parent
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.site_tariff,
        s.site_zone,
        s.read_at_month,
        s.kwh_plus_voltage,
        s.read_at_label,
        si.zone AS site_zone_2,
        si.installed_kw AS site_installed_kw
    FROM annotated s
    JOIN sites si ON si.id = s.site_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.site_tariff,
        s.site_zone,
        s.kwh_plus_voltage,
        s.read_at_label,
        si.site_name,
        si.installed_kw AS site_installed_kw_2,
        si.tariff AS site_tariff_2
    FROM with_details_site s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff <> 'EV'
),
site_id_stats AS (
    -- Avg kwh per site_id
    SELECT site_id, AVG(kwh) AS avg_kwh
    FROM combined_details
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM combined_details s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh > g.avg_kwh
),
with_details_site_2 AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.site_zone,
        s.read_at_label,
        s.site_installed_kw_2,
        s.site_tariff_2,
        s.avg_kwh,
        si.tariff AS site_tariff_3,
        si.zone AS site_zone_2
    FROM vs_group s
    JOIN sites si ON si.id = s.site_id
)
SELECT id, metric_name, reading
FROM (
    SELECT id, avg_kwh, site_installed_kw_2
    FROM with_details_site_2
) src
UNPIVOT (
    reading FOR metric_name IN (avg_kwh, site_installed_kw_2)
) unp
ORDER BY id, metric_name
