select * from (select firmware, device_code, humidity from device_readings) src pivot (sum(humidity) as amount, count(1) as cnt for device_code in ('DV-03' as dv_03, 'DV-04' as dv_04));
