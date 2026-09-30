WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'EST'
      AND voltage BETWEEN 216.1 AND 243.1
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 233.1
              AND m.kwh <= 109.3
        )
      AND s.site_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM with_activity
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE least_kwh_voltage > (SELECT AVG(least_kwh_voltage) FROM annotated)
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality_or_unknown,
        s.least_kwh_voltage,
        s.read_at_label,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM without_matches s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT *
FROM (
    SELECT read_at, reading_quality, reading_voltage
    FROM line_level_readings
) src
PIVOT (
    SUM(reading_voltage) AS amount, COUNT(reading_voltage) AS n
    FOR reading_quality IN ('EST', 'OK')
)
ORDER BY read_at
