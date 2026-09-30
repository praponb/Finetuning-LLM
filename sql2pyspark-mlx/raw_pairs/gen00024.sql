with valid as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        humidity / nullif(status_bits, 0) as humidity_per_status_bits
    from device_readings
    where reading_ts >= timestamp '2025-09-01 02:00:00'
      and humidity < 39
),
valid_device_readings as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        humidity_per_status_bits + humidity as humidity_per_status_bits_plus_humidity,
        upper(device_code) as device_code_upper
    from valid
    where temp_c < 34
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware
    from valid_device_readings
    qualify row_number() over (partition by device_code order by humidity_per_status_bits_plus_humidity, id) = 1
),
latest_device_readings as (
    -- QUALIFY-style window filter
    select
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware
    from (
        select *, row_number() over (partition by device_code order by humidity nulls last, id) as rn
        from latest
    ) x
    where rn = 1
)
-- Native SQL PIVOT
select *
from (
    select
        firmware,
        device_code,
        temp_c
    from latest_device_readings
) src
pivot (
    avg(temp_c) as value_sum
    for device_code in ('DV-02' as dv_02, 'DV-01' as dv_01)
);
