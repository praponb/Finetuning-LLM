SELECT
    tariff AS name,
    id AS entity_id,
    rate,
    'tariff' AS src
FROM tariff_windows
WHERE tariff = 'TOU'
UNION ALL
SELECT
    quality,
    site_id,
    voltage,
    'reading' AS src
FROM meter_readings
