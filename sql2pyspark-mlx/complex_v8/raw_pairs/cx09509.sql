SELECT
    site_id AS qty,
    read_at,
    voltage,
    'reading' AS src
FROM meter_readings
WHERE quality IN ('OK', 'BAD')
  AND voltage <= 235.8
UNION ALL
SELECT
    id AS qty,
    valid_from AS read_at,
    rate AS voltage,
    'tariff' AS src
FROM tariff_windows
UNION ALL
SELECT
    id AS qty,
    read_at,
    voltage,
    'reading_3' AS src
FROM meter_readings
WHERE quality = 'OK'
  AND site_id <= 9
