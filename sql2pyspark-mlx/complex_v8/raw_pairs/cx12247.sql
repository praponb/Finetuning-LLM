SELECT id, valid_to AS event_time
FROM tariff_windows
WHERE rate < 5.5
  AND valid_to >= TIMESTAMP '2025-01-25 21:00:00'
UNION ALL
SELECT site_id AS id, read_at AS event_time
FROM meter_readings
WHERE kwh > 52.4
  AND site_id <= 2
