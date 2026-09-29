SELECT site_id
FROM meter_readings
INTERSECT
SELECT id
FROM tariff_windows
WHERE rate > 3.7
  AND TO_DATE(valid_from) < DATE '2025-03-09'
ORDER BY site_id
