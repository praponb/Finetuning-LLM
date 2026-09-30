WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        TO_DATE(read_at) AS read_at_date,
        ABS(kwh - 58.5) AS kwh_distance
    FROM meter_readings
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality IN ('OK', 'EST')) AS reading_quality_count,
        MAX(voltage) AS max_reading_voltage,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.max_reading_voltage,
        COALESCE(m.reading_quality_count_2, 0) AS reading_quality_count_2
    FROM valid s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh >= 37.0
              AND m.quality IN ('OK', 'BAD')
        )
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff = 'FLAT'
              AND YEAR(si.commissioned_on) = 2024
        )
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    least_kwh_voltage,
    read_at_date,
    kwh_distance,
    reading_quality_count,
    max_reading_voltage,
    ROUND(reading_quality_count / NULLIF(kwh_distance, 0), 3) AS reading_quality_count_per_kwh_distance,
    ABS(max_reading_voltage - 222.2) AS max_reading_voltage_distance
FROM filtered
ORDER BY site_id
