with valid as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        humidity,
        status_bits,
        firmware,
        humidity * status_bits as humidity_times_status_bits
    from device_readings
    where reading_ts >= timestamp '2025-09-01 05:00:00'
)
-- Keep one row per key (latest first)
select *, row_number() over (partition by device_code order by humidity_times_status_bits nulls last, id) as rank_in_group
from valid
qualify rank_in_group = 1
