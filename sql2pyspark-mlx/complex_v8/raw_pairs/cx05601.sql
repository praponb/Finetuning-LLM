SELECT quality AS category, id AS qty, read_at
FROM meter_readings
WHERE quality = 'BAD'
  AND voltage >= 218.2
UNION
SELECT tariff AS category, id AS qty, valid_from AS read_at
FROM tariff_windows
