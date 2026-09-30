SELECT id, site_id
FROM meter_readings
UNION ALL
SELECT site_id, id
FROM meter_readings
WHERE voltage >= 223.0
  AND quality NOT IN ('BAD', 'OK')
