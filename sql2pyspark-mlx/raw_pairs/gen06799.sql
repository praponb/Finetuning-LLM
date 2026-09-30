select *
from (
    select
        id,
        device_code,
        humidity,
        pressure,
        temp_c
    from device_readings
) src
unpivot (
    val for measure in (humidity, pressure, temp_c)
)
