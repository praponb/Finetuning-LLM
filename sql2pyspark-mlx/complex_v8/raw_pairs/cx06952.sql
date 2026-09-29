SELECT tariff AS label, valid_to
FROM tariff_windows
WHERE HOUR(valid_to) BETWEEN 9 AND 11
  AND tariff = 'TOU'
UNION ALL
SELECT quality, read_at
FROM meter_readings
