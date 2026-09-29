SELECT quality AS label
FROM meter_readings
MINUS
SELECT tariff AS label
FROM tariff_windows
