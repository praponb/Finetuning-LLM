SELECT site_id, quality
FROM meter_readings
WHERE site_id >= 5
  AND quality = 'BAD'
INTERSECT
SELECT id AS site_id, site_name AS quality
FROM sites
ORDER BY site_id, quality
