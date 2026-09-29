SELECT id AS ref_id, valid_from AS event_time
FROM tariff_windows
UNION ALL
SELECT id, read_at
FROM meter_readings
WHERE quality <> 'OK'
  AND voltage BETWEEN 218.5 AND 237.5
ORDER BY event_time, ref_id
