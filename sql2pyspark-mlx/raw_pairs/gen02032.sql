select * from (select id, device_code, temp_c, pressure from device_readings) src unpivot (val for measure in (temp_c, pressure as PRESSURE_VAL))
