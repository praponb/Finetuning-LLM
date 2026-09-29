SELECT id, tariff AS label, valid_to AS event_time
FROM tariff_windows
WHERE HOUR(valid_to) BETWEEN 10 AND 14
UNION ALL
SELECT id, quality AS label, read_at AS event_time
FROM meter_readings
WHERE quality NOT IN ('BAD', 'EST')
  AND site_id IS NOT NULL
