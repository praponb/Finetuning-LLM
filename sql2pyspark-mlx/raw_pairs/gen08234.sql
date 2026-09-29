with cleaned as (
    select
        id,
        device_code,
        temp_c,
        status_bits,
        firmware,
        coalesce(humidity, 0) as humidity_filled,
        greatest(temp_c, humidity) as greatest_temp_c_humidity
    from device_readings
    where reading_ts > timestamp '2025-09-01 07:00:00'
)
select id, device_code
from cleaned
except
select id, device_code
from device_readings
where temp_c <= 39
