WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        voltage - kwh AS voltage_minus_kwh,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 13:00:00'
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_prefix,
        s.voltage_minus_kwh,
        s.read_at_week,
        si.zone AS site_zone
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(voltage) AS min_reading_voltage,
        COUNT(*) AS reading_count,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at
    FROM combined_details s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(site_zone, 'ALL') AS site_zone,
    GROUPING(quality) AS is_total_quality,
    ROUND(SUM(kwh), 2) AS total_kwh
FROM summarized_rows
GROUP BY GROUPING SETS ((quality, site_zone), (quality), ())
ORDER BY quality NULLS LAST, site_zone NULLS LAST
