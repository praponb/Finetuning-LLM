WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage < 237.5
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.site_name
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        quality,
        voltage_filled,
        site_installed_kw,
        site_name,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(voltage_per_kwh - AVG(voltage_per_kwh) OVER (PARTITION BY site_id), 2) AS voltage_per_kwh_vs_site_id_avg
    FROM with_details_site
    WINDOW w_site_id AS (PARTITION BY site_id)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MAX(read_at) AS last_reading_read_at_2,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.last_reading_read_at_2,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM ranked s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.voltage_filled,
        s.site_installed_kw,
        s.site_name,
        s.rows_in_site_id,
        s.last_reading_read_at,
        s.last_reading_read_at_2,
        s.reading_quality_count,
        si.tariff AS site_tariff
    FROM profiled s
    JOIN sites si ON si.id = s.site_id
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.site_installed_kw,
        s.site_name,
        s.rows_in_site_id,
        s.last_reading_read_at,
        s.last_reading_read_at_2,
        s.reading_quality_count,
        s.site_tariff,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw_2,
        si.commissioned_on AS site_commissioned_on
    FROM with_parent s
    JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT site_tariff, site_id, quality, site_installed_kw
    FROM with_parent_site
) src
PIVOT (
    COUNT(site_installed_kw)
    FOR quality IN ('OK', 'EST')
)
ORDER BY site_tariff, site_id
