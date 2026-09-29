SELECT id
FROM meter_readings
EXCEPT
SELECT id
FROM meter_readings
WHERE kwh < 113.0
ORDER BY id DESC
