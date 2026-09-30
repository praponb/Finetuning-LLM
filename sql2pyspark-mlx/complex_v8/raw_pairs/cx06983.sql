WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        ABS(kwh - 86.0) AS kwh_distance,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 6 AND 12 OR voltage < 237.5)
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh BETWEEN 45.0 AND 117.4
        )
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_slug,
        s.kwh_distance,
        s.voltage_plus_kwh,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM screened s
    LEFT JOIN sites si ON si.id = s.site_id
),
site_id_stats AS (
    -- Avg kwh per site_id
    SELECT site_id, AVG(kwh) AS avg_kwh
    FROM joined_site
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM joined_site s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh > g.avg_kwh
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.quality_slug,
        s.kwh_distance,
        s.voltage_plus_kwh,
        s.site_zone,
        s.site_tariff,
        s.avg_kwh,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM vs_group s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh > 15.9
),
quality_stats AS (
    -- Median voltage_plus_kwh per quality
    SELECT quality, PERCENTILE_APPROX(voltage_plus_kwh, 0.5) AS median_voltage_plus_kwh
    FROM with_children
    GROUP BY quality
),
vs_group_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_voltage_plus_kwh,
        ROUND(s.voltage_plus_kwh - g.median_voltage_plus_kwh, 2) AS voltage_plus_kwh_minus_median
    FROM with_children s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage_plus_kwh >= g.median_voltage_plus_kwh
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality,
    kwh_distance,
    site_zone,
    site_tariff,
    avg_kwh,
    reading_id,
    reading_kwh,
    reading_read_at,
    median_voltage_plus_kwh,
    voltage_plus_kwh_minus_median,
    UPPER(TRIM(site_tariff)) AS site_tariff_clean
FROM vs_group_2
ORDER BY site_zone, read_at NULLS LAST
