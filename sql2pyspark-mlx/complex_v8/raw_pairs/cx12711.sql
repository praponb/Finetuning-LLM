SELECT id AS key_id, tariff
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'R%'
  AND zone = 'Z2'
INTERSECT
SELECT id, quality
FROM meter_readings
