SELECT id AS key_id, site_id, kwh
FROM meter_readings
UNION
SELECT id AS key_id, site_id, kwh
FROM meter_readings
ORDER BY site_id DESC
