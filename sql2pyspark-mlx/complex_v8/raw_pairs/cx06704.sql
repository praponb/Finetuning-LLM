SELECT tariff AS category, valid_from, rate
FROM tariff_windows
WHERE tariff IN ('FLAT', 'TOU')
  AND rate < 5.9
UNION
SELECT quality AS category, read_at AS valid_from, voltage AS rate
FROM meter_readings
WHERE voltage > 220.2
  AND MONTH(read_at) IN (1, 2)
ORDER BY category
