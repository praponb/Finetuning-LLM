SELECT id, tariff AS label, valid_from
FROM tariff_windows
UNION
SELECT id, quality, read_at
FROM meter_readings
ORDER BY valid_from
