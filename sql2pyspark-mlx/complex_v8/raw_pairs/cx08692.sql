SELECT id
FROM meter_readings
EXCEPT
SELECT id
FROM tariff_windows
WHERE tariff IS NOT NULL
ORDER BY id
