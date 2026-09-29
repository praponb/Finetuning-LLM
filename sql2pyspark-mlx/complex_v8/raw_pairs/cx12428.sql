SELECT id AS qty
FROM meter_readings
WHERE site_id IS NOT NULL
INTERSECT
SELECT id AS qty
FROM tariff_windows
WHERE valid_to < TIMESTAMP '2025-03-09 04:00:00'
  AND tariff IN ('TOU', 'EV')
ORDER BY qty DESC
