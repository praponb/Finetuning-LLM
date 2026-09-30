SELECT id AS entity_id, quality AS name
FROM meter_readings
INTERSECT
SELECT id, tariff
FROM tariff_windows
WHERE rate > 4.2
ORDER BY entity_id DESC
