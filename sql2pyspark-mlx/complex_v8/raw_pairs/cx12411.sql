SELECT
    id AS key_id,
    tariff AS label,
    valid_to,
    'tariff' AS origin
FROM tariff_windows
WHERE rate BETWEEN 2.2 AND 5.9
UNION ALL
SELECT
    id,
    quality,
    read_at,
    'reading' AS origin
FROM meter_readings
WHERE site_id >= 8
  AND voltage <= 237.8
