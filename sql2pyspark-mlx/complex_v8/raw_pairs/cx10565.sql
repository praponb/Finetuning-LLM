WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh >= 45.3
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on < DATE '2025-05-07'
              AND si.tariff IS NOT NULL
        )
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM qualified s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
qualified_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM with_children
    WHERE reading_voltage > (SELECT MIN(reading_voltage) FROM with_children)
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM qualified_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 236.7
              AND m.quality = 'BAD'
        )
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    reading_id,
    reading_read_at,
    reading_quality,
    reading_kwh,
    SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
FROM without_matches
WHERE reading_kwh BETWEEN 42.1 AND 92.5
ORDER BY id DESC, reading_id
LIMIT 10
