SELECT site_id AS ref_id
FROM meter_readings
INTERSECT
SELECT id
FROM tariff_windows
