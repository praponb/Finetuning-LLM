SELECT id AS entity_id
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'R%'
  AND tariff <> 'TOU'
INTERSECT
SELECT id
FROM meter_readings
WHERE site_id >= 6
ORDER BY entity_id DESC
