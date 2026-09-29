SELECT id
FROM tariff_windows
INTERSECT
SELECT id
FROM meter_readings
ORDER BY id
