SELECT 'reading' AS record_type, site_id, voltage AS amount
FROM meter_readings
WHERE site_id >= 4
UNION ALL
SELECT 'tariff' AS record_type, id AS site_id, rate AS amount
FROM tariff_windows
ORDER BY amount DESC, record_type
