SELECT
    id,
    device_code,
    humidity,
    pressure,
    status_bits,
    firmware
FROM device_readings
QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) = 1;
