SELECT id AS qty, voltage, site_id AS key_id
FROM meter_readings
UNION ALL
SELECT site_id, kwh, id
FROM meter_readings
WHERE MONTH(read_at) IN (2, 3)
ORDER BY key_id DESC
