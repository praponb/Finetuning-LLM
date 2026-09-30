with valid as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        coalesce(pressure, 0) as pressure_filled,
        cast(temp_c as int) as temp_c_int
    from device_readings
    where reading_ts > to_date('2025-09-01')
)
select
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure_filled
from (
    select *, row_number() over (partition by device_code order by temp_c_int desc, id) as row_num
    from valid
) ranked
where row_num = 1
