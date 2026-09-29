SELECT id
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 5
  AND zone = 'Z3'
EXCEPT
SELECT id
FROM meter_readings
ORDER BY id DESC
