WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
    WHERE kwh < 82.9
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality_slug,
        s.least_voltage_kwh,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(kwh / NULLIF(least_voltage_kwh, 0), 3) AS kwh_per_least_voltage_kwh
    FROM with_details
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(kwh), 2) AS avg_reading_kwh,
        MAX(read_at) AS last_reading_read_at,
        MAX(voltage) AS max_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh,
        m.last_reading_read_at,
        m.max_reading_voltage
    FROM labeled s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    least_voltage_kwh,
    site_name,
    site_commissioned_on,
    site_installed_kw,
    kwh_per_least_voltage_kwh,
    avg_reading_kwh,
    last_reading_read_at,
    max_reading_voltage,
    SUBSTRING(TRIM(site_name), 1, 3) AS site_name_prefix,
    ABS(avg_reading_kwh - 42.0) AS avg_reading_kwh_distance
FROM with_stats
ORDER BY last_reading_read_at NULLS LAST, voltage
