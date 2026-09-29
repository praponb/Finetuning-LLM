SELECT site_id, quality
FROM meter_readings
WHERE quality = 'EST'
INTERSECT
SELECT site_id, quality
FROM meter_readings
