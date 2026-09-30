with filtered as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        round(pressure / humidity, 2) as pressure_per_humidity
    from device_readings
    where reading_ts > date '2025-09-01'
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        pressure + humidity as pressure_plus_humidity,
        pressure_per_humidity - pressure as pressure_per_humidity_minus_pressure
    from filtered
    where reading_ts >= timestamp '2025-09-01 08:00:00'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by device_code order by pressure, id) as event_seq,
        avg(humidity) over (partition by device_code order by pressure_per_humidity, id rows between 5 preceding and current row) as humidity_avg_5,
        count(temp_c) over (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row) as temp_c_count_15m
    from base
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(humidity_avg_5) over w_grp as humidity_avg_5_avg_3_prior,
        sum(pressure_per_humidity) over (partition by device_code order by pressure_plus_humidity, id rows between unbounded preceding and current row) as cumulative_pressure_per_humidity
    from with_windows
    where temp_c_count_15m <= 12
    window w_grp as (partition by year(reading_ts), device_code order by temp_c, id rows between 3 preceding and 1 preceding)
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '45' minute then 1 else 0 end as session_start_flag
    from scored
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by device_code order by humidity desc nulls last, id) as rn
        from sessions_device_readings
    ) ranked
    where rn = 1
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by device_code order by rn desc) as row_num
        from ranked
    ) ranked
    where row_num = 1
)
-- Aggregate with conditional metrics
select
    session_id,
    sort_array(collect_set(device_code)) as device_code_list,
    percentile_approx(temp_c_count_15m, 0.9) as temp_c_count_15m_p90,
    round(avg(temp_c), 2) as avg_temp_c,
    count(case when device_code = 'DV-04' then 1 end) as dv_04_count,
    round(avg(pressure), 2) as avg_pressure,
    sort_array(collect_set(device_code)) as device_code_list_2
from deduped
group by cube (session_id)
having temp_c_count_15m_p90 < 1000000
order by avg_temp_c desc;
