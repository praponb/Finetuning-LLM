with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        greatest(pressure, status_bits) as greatest_pressure_status_bits,
        round(temp_c, 2) as temp_c_rounded
    from device_readings
    where humidity > 66
)
-- Window metrics over several different partitions
select
    *,
    cume_dist() over (order by greatest_pressure_status_bits desc nulls last) as greatest_pressure_status_bits_cume_dist,
    avg(humidity) over (partition by device_code order by greatest_pressure_status_bits, id rows between 3 preceding and current row) as humidity_avg_3,
    first_value(greatest_pressure_status_bits, true) over (order by pressure, id rows between unbounded preceding and current row) as first_known_greatest_pressure_status_bits
from cleaned;
