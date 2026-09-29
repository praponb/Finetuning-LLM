SELECT quality, voltage, 'reading' AS source
FROM meter_readings
WHERE site_id >= 2
UNION ALL
SELECT tariff AS quality, rate AS voltage, 'tariff' AS source
FROM tariff_windows
WHERE valid_from < TIMESTAMP '2025-03-09 04:00:00'
