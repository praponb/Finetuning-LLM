SELECT * FROM (SELECT firmware, device_code, temp_c FROM device_readings) src PIVOT (SUM(temp_c) AS amount, COUNT(1) AS cnt FOR device_code IN ('DV-01' AS dv_01, 'DV-02' AS dv_02))
