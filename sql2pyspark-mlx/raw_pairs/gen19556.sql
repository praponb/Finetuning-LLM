select *
from (
    select
        id,
        device_code,
        humidity,
        temp_c,
        pressure
    from device_readings
) src
unpivot include nulls (
    metric_value for kpi in (humidity, temp_c, pressure)
);
