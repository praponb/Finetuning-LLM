WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 221.8 AND 235.8
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.site_name
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM with_parent s
    JOIN meter_readings m ON m.site_id = s.site_id
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage > 219.5
              AND m.kwh > 26.9
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(reading_kwh, reading_voltage) AS greatest_reading_kwh_reading_voltage
    FROM qualified
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, metric_name, amount
FROM (
    SELECT id, reading_id, voltage, reading_voltage, reading_kwh
    FROM scored
) src
UNPIVOT (
    amount FOR metric_name IN (voltage, reading_voltage, reading_kwh)
) unp
