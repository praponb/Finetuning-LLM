WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE quality IN ('OK', 'BAD')
      AND kwh <= 111.9
),
with_activity AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw < 373.3
        )
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM with_activity s
)
SELECT id, metric, value
FROM (
    SELECT id, max_reading_voltage, kwh, voltage
    FROM profiled
) src
UNPIVOT (
    value FOR metric IN (max_reading_voltage, kwh, voltage)
) unp
