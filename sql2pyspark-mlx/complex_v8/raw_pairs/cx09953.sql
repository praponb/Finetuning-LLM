SELECT 'reading' AS origin, site_id AS entity_id, quality AS category
FROM meter_readings
WHERE voltage <= 237.5
  AND read_at >= TIMESTAMP '2025-01-25 09:00:00'
UNION ALL
SELECT 'tariff' AS origin, id, tariff
FROM tariff_windows
