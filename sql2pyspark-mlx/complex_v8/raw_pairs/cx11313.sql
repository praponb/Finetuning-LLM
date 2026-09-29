WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM meter_readings
    WHERE voltage > 220.7
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.voltage_bucket,
        s.greatest_voltage_kwh,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM base s
    JOIN meter_readings m ON m.site_id = s.site_id
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.voltage_bucket,
        s.greatest_voltage_kwh,
        s.reading_id,
        s.reading_quality,
        s.reading_voltage,
        s.reading_read_at,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM line_level_readings s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    site_zone,
    reading_quality,
    MIN(reading_read_at) AS first_reading_read_at
FROM joined_site
GROUP BY GROUPING SETS ((site_zone, reading_quality), (site_zone), ())
ORDER BY site_zone NULLS LAST, reading_quality NULLS LAST
