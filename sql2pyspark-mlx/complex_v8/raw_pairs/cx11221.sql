WITH combined AS (
    -- Stack the branches
    SELECT id AS key_id, read_at AS event_time, voltage
    FROM meter_readings
    UNION ALL
    SELECT id AS key_id, valid_to AS event_time, rate AS voltage
    FROM tariff_windows
    WHERE tariff = 'TOU'
      AND MONTH(valid_from) IN (2, 3)
)
SELECT key_id, COUNT(*) AS n_rows, SUM(voltage) AS total_voltage, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY key_id
ORDER BY key_id
