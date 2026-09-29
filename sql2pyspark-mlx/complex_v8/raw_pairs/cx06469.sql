SELECT
    id,
    site_id AS qty,
    kwh AS value,
    'reading' AS source
FROM meter_readings
WHERE read_at < TIMESTAMP '2025-02-12 08:00:00'
UNION ALL
SELECT
    id,
    site_id,
    kwh,
    'reading_2' AS source
FROM meter_readings
WHERE voltage <= 238.3
  AND site_id IS NOT NULL
