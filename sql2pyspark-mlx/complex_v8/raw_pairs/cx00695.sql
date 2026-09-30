SELECT
    site_id AS entity_id,
    quality AS label,
    id,
    'reading' AS src
FROM meter_readings
WHERE read_at >= TIMESTAMP '2025-01-25 09:00:00'
UNION
SELECT
    site_id,
    quality,
    id,
    'reading_2' AS src
FROM meter_readings
ORDER BY label, src
