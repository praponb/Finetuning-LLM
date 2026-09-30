SELECT tariff AS category
FROM sites
WHERE zone IN ('Z4', 'Z2')
EXCEPT
SELECT quality AS category
FROM meter_readings
WHERE quality IN ('BAD', 'EST')
