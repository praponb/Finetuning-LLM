WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE site_id IS NOT NULL
)
SELECT id, metric_name, amount
FROM (
    SELECT id, kwh, voltage
    FROM base_readings
) src
UNPIVOT (
    amount FOR metric_name IN (kwh AS m_kwh, voltage AS m_voltage)
) unp
ORDER BY id, metric_name
