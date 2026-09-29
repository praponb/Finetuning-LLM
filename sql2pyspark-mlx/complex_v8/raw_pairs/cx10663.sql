SELECT site_id, quality AS name
FROM meter_readings
WHERE read_at < TIMESTAMP '2025-02-24 10:00:00'
EXCEPT
SELECT id AS site_id, quality AS name
FROM meter_readings
WHERE kwh BETWEEN 43.1 AND 81.3
ORDER BY site_id, name
