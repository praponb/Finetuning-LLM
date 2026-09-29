SELECT id, valid_from AS event_time, tariff AS label
FROM tariff_windows
WHERE HOUR(valid_from) BETWEEN 7 AND 9
  AND TO_DATE(valid_to) < DATE '2025-03-12'
UNION ALL
SELECT site_id, read_at, quality
FROM meter_readings
UNION ALL
SELECT id, valid_to, tariff
FROM tariff_windows
ORDER BY label, event_time
