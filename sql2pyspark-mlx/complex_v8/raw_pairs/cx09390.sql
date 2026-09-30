WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE quality = 'EST'
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM prepared s
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.max_reading_voltage,
        s.last_reading_read_at,
        si.zone AS site_zone
    FROM profiled s
    JOIN sites si ON si.id = s.site_id
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.max_reading_voltage,
        s.last_reading_read_at,
        s.site_zone,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone_2
    FROM joined_site s
    LEFT JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
        kwh - max_reading_voltage AS kwh_minus_max_reading_voltage,
        ROUND(site_installed_kw / NULLIF(max_reading_voltage, 0), 3) AS site_installed_kw_per_max_reading_voltage
    FROM enriched_site
    WHERE max_reading_voltage > 213.5
),
latest_per_key AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        max_reading_voltage,
        last_reading_read_at,
        site_zone,
        site_installed_kw,
        site_zone_2,
        quality_group,
        kwh_minus_max_reading_voltage,
        site_installed_kw_per_max_reading_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY last_reading_read_at DESC, id DESC) AS row_num
        FROM tagged
    ) ranked
    WHERE row_num = 1
),
site_zone_2_stats AS (
    -- Median max_reading_voltage per site_zone_2
    SELECT site_zone_2, PERCENTILE_APPROX(max_reading_voltage, 0.5) AS median_max_reading_voltage
    FROM latest_per_key
    GROUP BY site_zone_2
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_max_reading_voltage,
        ROUND(s.max_reading_voltage - g.median_max_reading_voltage, 2) AS max_reading_voltage_minus_median
    FROM latest_per_key s
    JOIN site_zone_2_stats g ON g.site_zone_2 = s.site_zone_2
    WHERE s.max_reading_voltage >= g.median_max_reading_voltage
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, median_max_reading_voltage, site_installed_kw_per_max_reading_voltage
    FROM outliers
) src
UNPIVOT (
    metric_value FOR measure IN (median_max_reading_voltage, site_installed_kw_per_max_reading_voltage)
) unp
