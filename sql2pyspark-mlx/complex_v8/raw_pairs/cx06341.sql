WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
    FROM sites
    WHERE installed_kw < 284.0
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.zone,
        s.tariff,
        s.installed_kw,
        s.zone_or_unknown,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM base s
    JOIN meter_readings m ON m.site_id = s.id
),
latest AS (
    -- Keep the latest row per reading_quality, tariff
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        zone_or_unknown,
        reading_id,
        reading_voltage,
        reading_quality,
        reading_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY reading_quality, tariff ORDER BY reading_voltage DESC, id DESC, reading_id DESC) AS rn
        FROM with_children_readings
    ) ranked
    WHERE rn = 1
),
with_details_reading AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.site_name,
        s.zone,
        s.installed_kw,
        s.zone_or_unknown,
        s.reading_id,
        s.reading_voltage,
        s.reading_quality,
        s.reading_kwh,
        m.kwh AS reading_kwh_2,
        m.site_id AS reading_site_id,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM latest s
    JOIN meter_readings m ON m.id = s.reading_id
    JOIN sites si ON si.id = m.site_id
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    installed_kw,
    reading_voltage,
    reading_quality,
    reading_kwh,
    reading_kwh_2,
    site_zone,
    site_installed_kw,
    COALESCE(reading_quality, 'UNKNOWN') AS reading_quality_or_unknown
FROM with_details_reading
WHERE site_zone IS NOT NULL
ORDER BY zone, reading_quality DESC
