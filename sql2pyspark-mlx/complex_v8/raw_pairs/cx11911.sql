SELECT site_id
FROM meter_readings
INTERSECT
SELECT id
FROM tariff_windows
ORDER BY site_id DESC
