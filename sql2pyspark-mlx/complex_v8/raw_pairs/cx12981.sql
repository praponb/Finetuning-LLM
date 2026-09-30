SELECT
    'reading' AS record_type,
    quality,
    voltage,
    read_at AS event_time
FROM meter_readings
UNION ALL
SELECT
    'reading_2' AS record_type,
    quality,
    voltage,
    read_at
FROM meter_readings
WHERE quality = 'BAD'
ORDER BY event_time DESC
