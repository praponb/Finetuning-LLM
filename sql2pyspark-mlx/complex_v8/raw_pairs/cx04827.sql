WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CASE WHEN voltage >= 232.7 THEN 'large' WHEN voltage >= 224.8 THEN 'medium' ELSE 'small' END AS voltage_tier,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh < 106.0
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_slug,
        s.quality_group,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM filtered_readings s
    JOIN sites si ON si.id = s.site_id
),
quality_group_stats AS (
    -- Median voltage per quality_group
    SELECT quality_group, PERCENTILE_APPROX(voltage, 0.5) AS median_voltage
    FROM with_details_site
    GROUP BY quality_group
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_voltage,
        ROUND(s.voltage - g.median_voltage, 2) AS voltage_minus_median
    FROM with_details_site s
    JOIN quality_group_stats g ON g.quality_group = s.quality_group
    WHERE s.voltage >= g.median_voltage
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_id,
        site_zone,
        MIN(voltage_minus_median) AS min_voltage_minus_median,
        COUNT(*) AS row_count
    FROM above_average
    WHERE quality_group IN ('group_a', 'other')
    GROUP BY DATE_TRUNC('MONTH', read_at), site_id, site_zone
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, read_at_month, site_zone, min_voltage_minus_median
    FROM summary
) src
PIVOT (
    MAX(min_voltage_minus_median)
    FOR site_zone IN ('Z2', 'Z3')
)
