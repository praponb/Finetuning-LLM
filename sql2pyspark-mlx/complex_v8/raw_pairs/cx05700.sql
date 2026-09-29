SELECT id, read_at, 'reading' AS source
FROM meter_readings
WHERE read_at >= TIMESTAMP '2025-01-19 13:00:00'
UNION ALL
SELECT id, read_at, 'reading_2' AS source
FROM meter_readings
WHERE kwh > 50.4
  AND voltage <= 236.7
UNION ALL
SELECT id, read_at, 'reading_3' AS source
FROM meter_readings
