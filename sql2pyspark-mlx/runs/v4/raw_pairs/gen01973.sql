select *
from (
    select
        id,
        device_code,
        temp_c,
        humidity,
        pressure
    from device_readings
) src
unpivot (
    metric_value for metric_name in (temp_c, humidity, pressure)
);
