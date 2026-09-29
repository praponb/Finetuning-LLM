SELECT id, kwh, site_id
FROM meter_readings
UNION ALL
SELECT id, kwh, site_id
FROM meter_readings
WHERE site_id <= 5
ORDER BY site_id DESC, kwh
