SELECT id, site_name
FROM sites
WHERE commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-07'
  AND tariff IS NOT NULL
INTERSECT
SELECT site_id AS id, quality AS site_name
FROM meter_readings
ORDER BY site_name, id
