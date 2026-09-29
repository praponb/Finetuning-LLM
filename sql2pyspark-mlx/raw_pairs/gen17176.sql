with filtered as (
    -- Keep recent, valid rows
    select *, cast(humidity as int) as humidity_int
    from device_readings
    where reading_ts > to_timestamp('2025-09-01 04:00:00')
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, pressure * temp_c as pressure_times_temp_c
    from filtered
    where reading_ts > to_date('2025-09-01')
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '15' minute then 1 else 0 end as is_new_session
    from valid
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        pressure_times_temp_c,
        sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 30 minutes then 1 else 0 end as is_new_session
    from sessionized
),
sessionized_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id_2
    from with_gaps
)
-- Aggregate with conditional metrics
select
    case when pressure_times_temp_c >= 65 then 'PREMIUM' else 'STANDARD' end as pressure_times_temp_c_tier,
    count(distinct device_code) as distinct_device_codes,
    sort_array(collect_set(device_code)) as device_code_list,
    round(avg(burst_id_2), 2) as avg_burst_id_2
from sessionized_device_readings
group by pressure_times_temp_c_tier
having count(distinct device_code) > 1
order by pressure_times_temp_c_tier;
