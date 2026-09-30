with ranked as (
    select *
    from device_readings
    qualify row_number() over (partition by device_code order by temp_c desc, id) = 1
)
select
    id,
    device_code,
    temp_c,
    pressure,
    status_bits,
    firmware
from ranked
order by id desc nulls last;
