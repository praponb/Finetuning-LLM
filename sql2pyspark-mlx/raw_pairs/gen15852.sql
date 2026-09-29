with base as (
    select *, lower(device_code) as device_code_lower
    from device_readings
    where reading_ts >= to_date('2025-09-01')
      and status_bits = 7
),
ranked as (
    select *
    from base
    qualify rank() over (partition by device_code order by status_bits) = 1
),
ranked_device_readings as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        device_code_lower
    from ranked
    qualify rank() over (partition by device_code order by reading_ts desc) = 1
),
with_prev as (
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from ranked_device_readings
),
gaps as (
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 1 hour then 1 else 0 end as new_group_flag
    from with_prev
),
sessionized as (
    select
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        device_code_lower,
        sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
base_device_readings as (
    select *, case when pressure >= 1018 then 'High' when pressure >= 987 then 'Medium' else 'Low' end as pressure_band
    from sessionized
    where reading_ts > date '2025-09-01'
),
kept_keys as (
    select id
    from base_device_readings
    except
    select id
    from device_readings
    where pressure between 1003 and 1018
),
kept_rows as (
    select s.*
    from base_device_readings s
    inner join kept_keys k on s.id = k.id
)
select
    reading_ts,
    pressure,
    burst_id
from kept_rows
order by burst_id desc
