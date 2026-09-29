SELECT site_id, id
FROM meter_readings
UNION ALL
SELECT site_id, id
FROM meter_readings
UNION ALL
SELECT id, site_id
FROM meter_readings
