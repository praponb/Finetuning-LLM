SELECT id AS key_id
FROM tariff_windows
INTERSECT
SELECT id AS key_id
FROM meter_readings
WHERE site_id IS NOT NULL
ORDER BY key_id
