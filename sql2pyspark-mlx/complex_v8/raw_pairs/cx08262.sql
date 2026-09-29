SELECT 'reading' AS record_type, quality AS label, voltage
FROM meter_readings
UNION ALL
SELECT 'reading_2' AS record_type, quality, kwh
FROM meter_readings
