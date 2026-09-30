SELECT site_id AS qty, read_at AS event_time, 'reading' AS origin
FROM meter_readings
WHERE site_id IS NOT NULL
UNION ALL
SELECT id, valid_to, 'tariff' AS origin
FROM tariff_windows
WHERE valid_from < TIMESTAMP '2025-03-09 04:00:00'
