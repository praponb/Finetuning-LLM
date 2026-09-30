SELECT id
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 8
  AND zone = 'Z4'
EXCEPT ALL
SELECT id
FROM meter_readings
ORDER BY id DESC
