SELECT site_id, quality
FROM meter_readings
WHERE quality NOT IN ('OK', 'BAD')
  AND site_id >= 2
EXCEPT
SELECT id, tariff
FROM tariff_windows
WHERE HOUR(valid_to) BETWEEN 10 AND 16
ORDER BY quality, site_id
