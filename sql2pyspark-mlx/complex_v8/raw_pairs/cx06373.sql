SELECT id, quality AS label
FROM meter_readings
WHERE voltage <= 238.3
EXCEPT
SELECT site_id, quality
FROM meter_readings
WHERE quality NOT IN ('EST', 'OK')
  AND site_id IS NOT NULL
ORDER BY label DESC
