SELECT * FROM (SELECT firmware, device_code, humidity FROM device_readings) src PIVOT (MAX(humidity) AS total FOR device_code IN ('DV-01' AS dv_01, 'DV-02' AS dv_02));
