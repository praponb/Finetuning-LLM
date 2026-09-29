SELECT * FROM (SELECT firmware, device_code, temp_c FROM device_readings) src PIVOT (AVG(temp_c) FOR device_code IN ('DV-01' AS dv_01, 'DV-02' AS dv_02))
