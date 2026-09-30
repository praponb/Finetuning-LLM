WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        kwh + voltage AS kwh_plus_voltage
    FROM meter_readings
    WHERE kwh >= 35.0
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.quality_prefix,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM valid_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage < 233.6
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', reading_read_at) AS days_since_reading_read_at
    FROM exploded_detail_readings
    WHERE reading_read_at < TIMESTAMP '2025-03-02 09:00:00'
),
enriched AS (
    -- Attach reading attributes
    SELECT
        s.*,
        m.read_at AS reading_read_at_2,
        m.site_id AS reading_site_id,
        m.voltage AS reading_voltage_2
    FROM scored s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reading_read_at, quality, reading_kwh
    FROM enriched
) src
PIVOT (
    AVG(reading_kwh)
    FOR quality IN ('EST', 'BAD')
)
ORDER BY reading_read_at
