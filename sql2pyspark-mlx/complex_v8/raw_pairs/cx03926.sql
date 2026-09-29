WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 80.8
        )
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM screened s
),
with_activity AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw < 386.6
              AND si.commissioned_on >= DATE '2024-06-27'
        )
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_year,
        s.voltage_filled,
        s.reading_count,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM with_activity s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    read_at_year,
    reading_count,
    site_name,
    site_installed_kw,
    ABS(reading_count - 4) AS reading_count_distance,
    CASE WHEN voltage_filled >= 234.5 THEN 'HIGH' WHEN voltage_filled >= 218.8 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_filled_tier
FROM with_parent_site
ORDER BY quality, reading_count_distance
