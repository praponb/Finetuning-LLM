WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE quality = 'BAD'
      AND voltage BETWEEN 219.6 AND 238.3
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(voltage) AS min_reading_voltage,
        COUNT(*) AS reading_count,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM scoped s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality IS NOT NULL
              AND m.read_at < TIMESTAMP '2025-03-08 10:00:00'
        )
      AND s.quality <> 'OK'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.voltage_plus_kwh,
        s.min_reading_voltage,
        si.tariff AS site_tariff,
        si.site_name
    FROM qualified s
    LEFT JOIN sites si ON si.id = s.site_id
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh >= 35.9
              AND m.quality IS NOT NULL
        )
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        min_reading_voltage,
        site_name,
        SUM(voltage) OVER (PARTITION BY site_tariff, site_id) AS site_tariff_site_id_sum_voltage,
        ROUND(min_reading_voltage - AVG(min_reading_voltage) OVER (PARTITION BY site_tariff, site_id), 2) AS min_reading_voltage_vs_site_tariff_site_id_avg,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY min_reading_voltage NULLS LAST, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY min_reading_voltage NULLS LAST, id), 3) AS min_reading_voltage_cume_dist
    FROM screened
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM ranked s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_tariff_site_id_sum_voltage,
    min_reading_voltage_vs_site_tariff_site_id_avg,
    min_reading_voltage_cume_dist,
    site_installed_kw,
    site_tariff,
    LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
FROM combined_details_site
ORDER BY min_reading_voltage_vs_site_tariff_site_id_avg NULLS LAST, id
LIMIT 10
