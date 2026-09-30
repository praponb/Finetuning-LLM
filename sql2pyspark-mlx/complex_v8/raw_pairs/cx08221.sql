WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        CASE WHEN voltage >= 232.4 THEN 'large' WHEN voltage >= 224.9 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
    WHERE kwh < 79.4
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(voltage) AS min_reading_voltage,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage,
        m.last_reading_read_at
    FROM cleaned_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
quality_group_stats AS (
    -- Avg voltage per quality_group
    SELECT quality_group, AVG(voltage) AS avg_voltage
    FROM with_counts
    GROUP BY quality_group
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM with_counts s
    JOIN quality_group_stats g ON g.quality_group = s.quality_group
    WHERE s.voltage > g.avg_voltage
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    quality,
    kwh_per_voltage,
    voltage_tier,
    min_reading_voltage,
    last_reading_read_at,
    avg_voltage,
    CONCAT(voltage_tier, '#', CAST(site_id AS STRING)) AS voltage_tier_ref
FROM outliers
