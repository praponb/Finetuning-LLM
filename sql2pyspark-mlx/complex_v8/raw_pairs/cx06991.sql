SELECT
    'reading' AS source,
    id AS entity_id,
    read_at,
    quality
FROM meter_readings
WHERE voltage > 221.4
  AND quality IN ('EST', 'OK')
UNION ALL
SELECT
    'reading_2' AS source,
    id,
    read_at,
    quality
FROM meter_readings
ORDER BY source
