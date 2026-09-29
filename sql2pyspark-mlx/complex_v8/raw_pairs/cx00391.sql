WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        ABS(kwh - 74.3) AS kwh_distance
    FROM meter_readings
    WHERE site_id >= 5
      AND quality = 'OK'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(kwh_distance / NULLIF(voltage, 0), 3) AS kwh_distance_per_voltage
    FROM base
    WHERE read_at_quarter > 1
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh
    FROM enriched s
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_quarter,
        kwh_distance,
        kwh_distance_per_voltage,
        reading_count,
        min_reading_kwh,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        LENGTH(TRIM(quality_group)) AS quality_group_len
    FROM with_stats
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.read_at_quarter,
        s.kwh_distance,
        s.kwh_distance_per_voltage,
        s.reading_count,
        s.min_reading_kwh,
        s.voltage_per_kwh,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM tagged s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage > 222.6
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.read_at_quarter,
        s.kwh_distance,
        s.kwh_distance_per_voltage,
        s.voltage_per_kwh,
        s.reading_id,
        s.reading_voltage,
        s.reading_quality,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM line_level_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE YEAR(si.commissioned_on) = 2025
),
deduped AS (
    -- Keep the latest row per reading_quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        read_at_quarter,
        kwh_distance,
        kwh_distance_per_voltage,
        voltage_per_kwh,
        reading_id,
        reading_voltage,
        reading_quality,
        site_commissioned_on,
        site_installed_kw
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY reading_quality, site_id ORDER BY site_commissioned_on DESC, id DESC, reading_id DESC) AS row_num
        FROM with_parent
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at_quarter, read_at, reading_quality, site_installed_kw
    FROM deduped
) src
PIVOT (
    MAX(site_installed_kw)
    FOR reading_quality IN ('BAD', 'EST', 'OK')
)
