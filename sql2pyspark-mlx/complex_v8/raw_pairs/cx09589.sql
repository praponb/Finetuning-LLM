SELECT site_id AS qty, quality AS name
FROM meter_readings
UNION ALL
SELECT site_id, quality
FROM meter_readings
ORDER BY qty DESC, name
