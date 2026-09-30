WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR TO_DATE(read_at) < DATE '2025-02-18')
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM prepared s
    JOIN meter_readings m ON m.site_id = s.site_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(kwh, reading_voltage) AS greatest_kwh_reading_voltage,
        CONCAT(reading_quality, '#', CAST(id AS STRING)) AS reading_quality_ref
    FROM exploded_detail
    WHERE kwh BETWEEN 30.2 AND 92.5
),
joined_reading AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.reading_id,
        s.reading_voltage,
        m.kwh AS reading_kwh,
        m.site_id AS reading_site_id,
        m.voltage AS reading_voltage_2
    FROM labeled s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
)
-- Final projection
SELECT DISTINCT kwh, reading_id
FROM joined_reading
ORDER BY kwh
