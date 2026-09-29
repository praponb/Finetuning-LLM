SELECT id, site_id AS entity_id, read_at
FROM meter_readings
WHERE quality NOT IN ('OK', 'EST')
UNION ALL
SELECT id, site_id AS entity_id, read_at
FROM meter_readings
UNION ALL
SELECT id, site_id AS entity_id, read_at
FROM meter_readings
WHERE quality = 'BAD'
ORDER BY id, read_at
