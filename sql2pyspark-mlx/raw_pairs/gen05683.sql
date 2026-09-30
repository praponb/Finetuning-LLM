select id, device_code, metric_name, value from (select id, device_code, pressure, humidity, temp_c from device_readings) src unpivot (value for metric_name in (pressure, humidity, temp_c))
