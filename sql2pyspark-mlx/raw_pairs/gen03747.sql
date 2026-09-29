select * from (select firmware, device_code, status_bits from device_readings) src pivot (avg(status_bits) as amount, count(1) as n for device_code in ('DV-01' as dv_01, 'DV-02' as dv_02))
