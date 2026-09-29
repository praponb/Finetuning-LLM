SELECT id
FROM sites
WHERE LOWER(site_name) LIKE '%al%'
  AND tariff IN ('EV', 'FLAT')
INTERSECT
SELECT site_id
FROM meter_readings
