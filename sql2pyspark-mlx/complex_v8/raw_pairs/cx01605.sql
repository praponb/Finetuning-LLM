SELECT site_id AS key_id
FROM meter_readings
WHERE quality IN ('EST', 'OK')
  AND read_at >= TIMESTAMP '2025-01-31 12:00:00'
EXCEPT
SELECT id
FROM sites
ORDER BY key_id
