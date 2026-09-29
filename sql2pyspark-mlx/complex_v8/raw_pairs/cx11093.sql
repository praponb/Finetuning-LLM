SELECT tariff AS name
FROM tariff_windows
WHERE tariff = 'EV'
  AND MONTH(valid_from) IN (2, 3)
EXCEPT
SELECT quality
FROM meter_readings
ORDER BY name DESC
