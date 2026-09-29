SELECT id, quality
FROM meter_readings
WHERE voltage > 215.6
EXCEPT
SELECT site_id AS id, quality
FROM meter_readings
ORDER BY id DESC
