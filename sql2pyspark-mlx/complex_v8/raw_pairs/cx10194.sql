SELECT 'reading' AS src, id, read_at
FROM meter_readings
UNION
SELECT 'reading_2' AS src, id, read_at
FROM meter_readings
ORDER BY read_at DESC, src
