with base as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        pressure + status_bits as pressure_plus_status_bits
    from device_readings
    where reading_ts >= timestamp '2025-09-01 05:00:00'
      and temp_c <= 24
),
remaining_keys as (
    -- Set intersection on keys
    select device_code, id
    from base
    intersect
    select device_code, id
    from device_readings
    where pressure between 995 and 1005
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join remaining_keys k on s.device_code = k.device_code and s.id = k.id
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        humidity,
        pressure,
        status_bits,
        pressure_plus_status_bits,
        pressure - status_bits as pressure_minus_status_bits,
        case when device_code = 'DV-03' then 1 else 0 end as is_dv_03
    from kept_rows
    where pressure <= 996
),
base_device_readings as (
    -- Basic cleansing and defaults
    select
        id,
        humidity,
        pressure,
        pressure_minus_status_bits,
        is_dv_03,
        ceil(pressure_minus_status_bits) as pressure_minus_status_bits_whole,
        round(humidity, 0) as humidity_rounded
    from valid
    where status_bits <= 9
)
-- Multi-level aggregation
select
    case when pressure_minus_status_bits >= 87 then 'HIGH' else 'LOW' end as pressure_minus_status_bits_tier,
    case when pressure >= 1018 then 'LARGE' else 'SMALL' end as pressure_tier,
    case when pressure_minus_status_bits_whole >= 82 then 'HIGH' else 'LOW' end as pressure_minus_status_bits_whole_tier,
    count(distinct id) as distinct_ids,
    sum(is_dv_03) as total_is_dv_03,
    count(distinct id) as distinct_ids_2
from base_device_readings
group by case when pressure_minus_status_bits >= 87 then 'HIGH' else 'LOW' end, case when pressure >= 1018 then 'LARGE' else 'SMALL' end, case when pressure_minus_status_bits_whole >= 82 then 'HIGH' else 'LOW' end
having count(distinct id) < 1000;
