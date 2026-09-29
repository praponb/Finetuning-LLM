WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE site_id <= 3
      AND voltage BETWEEN 220.7 AND 233.1
),
filtered_2 AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-01-13 13:00:00'
        )
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM filtered_2 s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone = 'Z4'
              AND si.installed_kw > 141.6
        )
),
filtered_3 AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND HOUR(m.read_at) BETWEEN 6 AND 12
              AND m.kwh >= 48.2
        )
),
joined AS (
    -- Attach site attributes
    SELECT s.*, si.tariff AS site_tariff, si.commissioned_on AS site_commissioned_on
    FROM filtered_3 s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, kwh, voltage
    FROM joined
) src
UNPIVOT (
    metric_value FOR measure IN (kwh AS m_kwh, voltage AS m_voltage)
) unp
ORDER BY id, measure
