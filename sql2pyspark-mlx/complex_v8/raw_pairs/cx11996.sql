WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh >= 50.4
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM cleaned_readings
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_per_voltage,
        s.quality_group,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM derived s
    JOIN meter_readings m ON m.site_id = s.site_id
),
reading_quality_stats AS (
    -- Max kwh per reading_quality
    SELECT reading_quality, MAX(kwh) AS max_kwh
    FROM exploded_detail_readings
    GROUP BY reading_quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM exploded_detail_readings s
    JOIN reading_quality_stats g ON g.reading_quality = s.reading_quality
    WHERE s.kwh = g.max_kwh
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_per_voltage,
        s.quality_group,
        s.reading_id,
        s.reading_quality,
        s.kwh_minus_max,
        si.zone AS site_zone,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM above_average s
    LEFT JOIN sites si ON si.id = s.site_id
),
derived_2 AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        voltage,
        quality,
        quality_group,
        reading_id,
        reading_quality,
        kwh_minus_max,
        site_name,
        kwh_per_voltage + kwh_minus_max AS kwh_per_voltage_plus_kwh_minus_max,
        CONCAT_WS('-', quality_group, site_zone) AS quality_group_site_zone_label
    FROM with_details_site
)
SELECT *
FROM (
    SELECT quality, reading_quality, kwh_per_voltage_plus_kwh_minus_max
    FROM derived_2
) src
PIVOT (
    AVG(kwh_per_voltage_plus_kwh_minus_max) AS avg_v, MAX(kwh_per_voltage_plus_kwh_minus_max) AS max_v
    FOR reading_quality IN ('OK', 'BAD', 'EST')
)
ORDER BY quality
