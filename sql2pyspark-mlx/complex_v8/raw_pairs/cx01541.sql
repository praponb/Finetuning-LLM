SELECT site_id AS qty
FROM meter_readings
WHERE quality <> 'BAD'
  AND voltage >= 218.5
INTERSECT
SELECT id
FROM sites
ORDER BY qty DESC
