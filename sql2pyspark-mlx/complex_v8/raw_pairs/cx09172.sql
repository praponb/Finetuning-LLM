SELECT site_id
FROM meter_readings
EXCEPT ALL
SELECT id
FROM meter_readings
ORDER BY site_id DESC
