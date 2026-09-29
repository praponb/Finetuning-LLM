SELECT id, site_id AS key_id, voltage
FROM meter_readings
WHERE quality <> 'OK'
UNION ALL
SELECT id, site_id AS key_id, voltage
FROM meter_readings
