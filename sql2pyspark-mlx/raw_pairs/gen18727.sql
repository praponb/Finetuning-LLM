SELECT * FROM (SELECT firmware, device_code, humidity FROM device_readings) src PIVOT (AVG(humidity) AS amount FOR device_code IN ('DV-04' AS dv_04, 'DV-03' AS dv_03))
