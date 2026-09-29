with valid as (
    select *, dayofweek(reading_ts) reading_ts_dayofweek
    from device_readings
    where reading_ts >= date '2025-09-01'
)
select *
from (
    select
        id,
        device_code,
        humidity,
        pressure,
        temp_c
    from valid
) src
unpivot (
    val for kpi in (humidity, pressure as PRESSURE, temp_c as TEMP_C)
);
