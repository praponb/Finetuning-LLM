WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE voltage < 232.7
      AND HOUR(read_at) BETWEEN 9 AND 11
)
SELECT
    'reading' AS origin,
    id AS entity_id,
    site_id,
    read_at
FROM prepared
WHERE read_at >= TIMESTAMP '2025-01-25 11:00:00'
  AND quality = 'OK'
UNION
SELECT
    'reading_2' AS origin,
    id AS entity_id,
    site_id,
    read_at
FROM meter_readings
WHERE TO_DATE(read_at) < DATE '2025-03-08'
ORDER BY read_at DESC
