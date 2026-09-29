SELECT tariff, valid_to AS event_time, 'tariff' AS source
FROM tariff_windows
UNION ALL
SELECT quality, read_at, 'reading' AS source
FROM meter_readings
WHERE HOUR(read_at) BETWEEN 7 AND 9
  AND quality = 'OK'
