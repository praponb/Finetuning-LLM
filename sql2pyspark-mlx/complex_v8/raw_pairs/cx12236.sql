SELECT id
FROM sites
INTERSECT ALL
SELECT site_id AS id
FROM meter_readings
ORDER BY id DESC
