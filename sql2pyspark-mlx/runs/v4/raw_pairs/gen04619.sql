with base as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        length(device_code) as device_code_length
    from device_readings
    where reading_ts > timestamp '2025-09-01 04:00:00'
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by device_code order by reading_ts, id) as latest_rank
        from base
    ) ranked
    where latest_rank = 1
),
filtered as (
    -- Keep recent, valid rows
    select *, pressure + temp_c as pressure_plus_temp_c
    from ranked
    where reading_ts >= timestamp '2025-09-01 13:00:00'
)
select
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    device_code_length,
    pressure_plus_temp_c
from filtered;
