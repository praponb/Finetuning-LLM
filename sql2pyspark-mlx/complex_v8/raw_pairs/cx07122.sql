SELECT tariff AS category, valid_to AS event_time, 'tariff' AS origin
FROM tariff_windows
UNION ALL
SELECT quality AS category, read_at AS event_time, 'reading' AS origin
FROM meter_readings
WHERE site_id IS NOT NULL
ORDER BY origin
