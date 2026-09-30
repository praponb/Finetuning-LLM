SELECT site_id, quality
FROM meter_readings
WHERE kwh <= 109.3
MINUS
SELECT site_id, quality
FROM meter_readings
ORDER BY site_id DESC, quality
