WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage - kwh AS voltage_minus_kwh,
        QUARTER(read_at) AS read_at_quarter,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 8 AND 14
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.voltage_minus_kwh,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw < 284.0
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.voltage_minus_kwh,
        s.site_installed_kw,
        s.site_name,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM enriched_site s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at >= TIMESTAMP '2025-01-25 11:00:00'
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        quality,
        voltage_minus_kwh,
        site_installed_kw,
        site_name,
        reading_id,
        reading_quality,
        reading_voltage,
        ROUND(reading_voltage / NULLIF(reading_kwh, 0), 3) AS reading_voltage_per_reading_kwh
    FROM line_level
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(quality, 'ALL') AS quality,
    reading_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt,
    SUM(reading_voltage_per_reading_kwh) FILTER (WHERE reading_quality = 'OK') AS reading_voltage_per_reading_kwh_reading_quality_
FROM tagged
GROUP BY ROLLUP(quality, reading_id)
ORDER BY quality NULLS LAST, reading_id NULLS LAST
