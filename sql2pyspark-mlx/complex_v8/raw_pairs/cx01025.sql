SELECT id, quality AS category
FROM meter_readings
EXCEPT
SELECT site_id, quality
FROM meter_readings
WHERE site_id IS NOT NULL
