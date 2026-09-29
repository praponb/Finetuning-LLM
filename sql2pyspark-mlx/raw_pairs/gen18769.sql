SELECT * FROM (SELECT firmware, device_code, status_bits FROM device_readings) src PIVOT (AVG(status_bits) AS value_sum FOR device_code IN ('DV-02' AS dv_02, 'DV-04' AS dv_04, 'DV-01' AS dv_01))
