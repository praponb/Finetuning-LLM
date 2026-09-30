WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE voltage BETWEEN 219.6 AND 233.1
),
site_id_stats AS (
    -- Avg greatest_kwh_voltage per site_id
    SELECT site_id, AVG(greatest_kwh_voltage) AS avg_greatest_kwh_voltage
    FROM base_readings
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_greatest_kwh_voltage,
        ROUND(s.greatest_kwh_voltage - g.avg_greatest_kwh_voltage, 2) AS greatest_kwh_voltage_minus_avg
    FROM base_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.greatest_kwh_voltage > g.avg_greatest_kwh_voltage
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.greatest_kwh_voltage,
        s.avg_greatest_kwh_voltage,
        s.greatest_kwh_voltage_minus_avg,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM outliers s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with CUBE
SELECT
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(site_zone, 'ALL') AS site_zone,
    site_id,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM enriched
GROUP BY CUBE(quality, site_zone, site_id)
