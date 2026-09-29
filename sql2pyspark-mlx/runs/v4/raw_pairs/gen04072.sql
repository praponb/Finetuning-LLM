with filtered as (
    -- Keep recent, valid rows
    select
        *,
        upper(device_code) as device_code_upper,
        datediff(current_date(), reading_ts) as days_since_reading_ts
    from device_readings
    where humidity > 35
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '45' minute then 1 else 0 end as is_new_burst
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '1' hour then 1 else 0 end as is_new_burst_2
    from sessionized
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst_2) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
with_gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 1 hour then 1 else 0 end as new_group_flag
    from sessions_device_readings
),
sessions_device_readings_2 as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id_2
    from with_gaps_2
),
gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 45 minutes then 1 else 0 end as is_new_burst_3
    from sessions_device_readings_2
),
sessionized_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst_3) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id_3
    from gaps_2
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        device_code_upper,
        days_since_reading_ts,
        is_new_burst,
        burst_id,
        is_new_burst_2,
        island_id,
        new_group_flag,
        burst_id_2,
        is_new_burst_3,
        burst_id_3,
        last_value(temp_c, true) over (order by days_since_reading_ts, id rows between unbounded preceding and current row) as last_known_temp_c,
        row_number() over w_grp as seq_no,
        percent_rank() over w_grp_2 as pressure_percent_rank,
        sum(burst_id) over (partition by device_code order by cast(reading_ts as bigint) range between 7200 preceding and current row) as burst_id_sum_2h,
        sum(temp_c) over w_running as running_temp_c
    from sessionized_device_readings
    window w_grp as (partition by device_code order by status_bits desc, id), w_grp_2 as (partition by device_code order by pressure desc nulls first), w_running as (partition by device_code order by is_new_burst_3, id rows between unbounded preceding and current row)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by device_code order by running_temp_c desc nulls first, id) as row_num
        from with_windows
    ) ranked
    where row_num = 1
)
-- Grouped metrics
select
    case when humidity >= 73 then 'PREMIUM' else 'STANDARD' end as humidity_tier,
    island_id,
    percentile_approx(pressure, 0.5) as pressure_p50,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes,
    percentile_approx(burst_id_3, array(0.25, 0.5, 0.75)) as burst_id_3_quartiles,
    max(days_since_reading_ts) filter (where device_code = 'DV-01') as dv_01_max_days_since_reading_ts
from latest
group by humidity_tier, island_id
order by humidity_tier desc
