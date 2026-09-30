select id, device_code, humidity, pressure from device_readings qualify row_number() over (partition by device_code order by humidity nulls first, id) = 1;
