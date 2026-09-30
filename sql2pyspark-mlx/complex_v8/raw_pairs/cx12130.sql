WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM meter_readings
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.greatest_voltage_kwh,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM valid_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM with_parent_site s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, site_tariff, quality, kwh
    FROM profiled
) src
PIVOT (
    AVG(kwh)
    FOR quality IN ('EST', 'OK', 'BAD')
)
ORDER BY site_id, site_tariff
