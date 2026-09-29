SELECT site_id AS key_id
FROM meter_readings
WHERE site_id <= 3
  AND HOUR(read_at) BETWEEN 6 AND 9
EXCEPT
SELECT id
FROM sites
WHERE installed_kw <= 373.3
ORDER BY key_id DESC
