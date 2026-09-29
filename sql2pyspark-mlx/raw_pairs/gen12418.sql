select *
from (
    select
        firmware,
        device_code,
        temp_c
    from device_readings
) src
pivot (
    max(temp_c) as value_sum
    for device_code in ('DV-02' as dv_02, 'DV-03' as dv_03, 'DV-04' as dv_04)
)
