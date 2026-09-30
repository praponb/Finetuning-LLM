WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE kwh < 113.0
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_prefix,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM filtered_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, ROUND(AVG(kwh), 2) AS avg_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh
    FROM exploded_detail s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    reading_id,
    site_id,
    COUNT(*) AS row_count,
    ROUND(SUM(kwh) / NULLIF(SUM(reading_voltage), 0), 3) AS kwh_to_reading_voltage_ratio,
    ROUND(SUM(reading_kwh) / NULLIF(SUM(reading_voltage), 0), 3) AS reading_kwh_to_reading_voltage_ratio
FROM with_counts
GROUP BY GROUPING SETS ((reading_id, site_id), (reading_id), (site_id))
