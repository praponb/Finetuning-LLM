WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.07, 2) AS kwh_adj,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-12 08:00:00'
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.voltage_minus_kwh,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM valid_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IN ('OK', 'EST')
              AND m.read_at >= TIMESTAMP '2025-01-25 09:00:00'
        )
),
enriched_reading AS (
    -- Attach reading attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.voltage_minus_kwh,
        s.reading_id,
        s.reading_quality,
        s.reading_read_at,
        s.reading_voltage,
        m.site_id AS reading_site_id,
        m.kwh AS reading_kwh,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM screened s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
    JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Attach site attributes
    SELECT s.*, si.site_name
    FROM enriched_reading s
    JOIN sites si ON si.id = s.reading_site_id
),
with_children_2 AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.voltage_minus_kwh,
        s.reading_id,
        s.reading_quality,
        s.reading_read_at,
        s.reading_voltage,
        s.reading_site_id,
        s.reading_kwh,
        s.site_zone,
        s.site_commissioned_on,
        s.site_name,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at_2,
        m.voltage AS reading_voltage_2,
        m.quality AS reading_quality_2
    FROM enriched s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    voltage_minus_kwh,
    reading_id,
    reading_read_at,
    reading_site_id,
    reading_kwh,
    site_zone,
    site_commissioned_on,
    site_name,
    reading_id_2,
    reading_read_at_2,
    reading_voltage_2
FROM with_children_2
ORDER BY site_name DESC, id, reading_id, reading_id_2
LIMIT 5
