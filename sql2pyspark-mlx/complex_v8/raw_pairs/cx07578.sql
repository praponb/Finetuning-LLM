SELECT quality AS name
FROM meter_readings
WHERE quality IS NOT NULL
  AND site_id IS NOT NULL
INTERSECT
SELECT tariff
FROM tariff_windows
ORDER BY name DESC
