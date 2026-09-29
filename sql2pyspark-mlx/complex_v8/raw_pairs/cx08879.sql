SELECT quality AS label, voltage
FROM meter_readings
WHERE voltage <= 233.1
UNION ALL
SELECT quality, voltage
FROM meter_readings
ORDER BY label
