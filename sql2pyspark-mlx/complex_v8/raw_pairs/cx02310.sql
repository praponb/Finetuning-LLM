SELECT site_id AS qty, read_at, 'reading' AS origin
FROM meter_readings
UNION
SELECT id, valid_from, 'tariff' AS origin
FROM tariff_windows
ORDER BY qty DESC, origin
