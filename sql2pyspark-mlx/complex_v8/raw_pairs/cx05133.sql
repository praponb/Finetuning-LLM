WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE quality IS NOT NULL
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM scoped_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 109.0
              AND m.quality = 'EST'
        )
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_clean,
        si.site_name,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM without_matches s
    LEFT JOIN sites si ON si.id = s.site_id
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.site_name,
        s.site_commissioned_on,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM joined_site s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality = 'EST'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        kwh,
        voltage,
        quality,
        site_commissioned_on,
        reading_id,
        reading_quality,
        ABS(kwh - 79.7) AS kwh_distance,
        reading_read_at + INTERVAL 2 HOURS AS reading_read_at_plus_2h,
        CONCAT(reading_quality, '#', CAST(site_id AS STRING)) AS reading_quality_ref
    FROM line_level
),
quality_stats AS (
    -- Avg voltage per quality
    SELECT quality, AVG(voltage) AS avg_voltage
    FROM scored
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM scored s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage > g.avg_voltage
),
enriched AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.voltage,
        s.site_commissioned_on,
        s.reading_id,
        s.reading_quality,
        s.kwh_distance,
        s.reading_read_at_plus_2h,
        s.voltage_minus_avg,
        m.site_id AS reading_site_id,
        si.site_name
    FROM above_average s
    JOIN meter_readings m ON m.id = s.reading_id
    LEFT JOIN sites si ON si.id = m.site_id
)
-- Final projection
SELECT
    id,
    voltage,
    site_commissioned_on,
    reading_id,
    reading_quality,
    voltage_minus_avg,
    FLOOR(voltage / 5) * 5 AS voltage_bucket
FROM enriched
ORDER BY voltage, id, reading_id
LIMIT 10
