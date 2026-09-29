SELECT id, site_id
FROM meter_readings
WHERE voltage >= 215.1
UNION ALL
SELECT id, site_id
FROM meter_readings
WHERE read_at < TIMESTAMP '2025-03-08 10:00:00'
