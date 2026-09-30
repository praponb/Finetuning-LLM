WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 233.1
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw BETWEEN 96.9 AND 446.9
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM combined_details_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 231.7
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM qualified s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Subtotals with CUBE
SELECT
    site_id,
    quality,
    site_zone,
    ROUND(SUM(voltage) / NULLIF(SUM(site_installed_kw), 0), 3) AS voltage_to_site_installed_kw_ratio,
    MIN(kwh) AS min_kwh
FROM with_stats
GROUP BY CUBE(site_id, quality, site_zone)
ORDER BY site_id NULLS LAST, quality NULLS LAST, site_zone NULLS LAST
