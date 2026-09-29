select * from (select id, device_code, pressure, temp_c, humidity from device_readings) src unpivot (metric_value for measure in (pressure, temp_c, humidity));
