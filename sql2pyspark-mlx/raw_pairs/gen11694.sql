with filtered as (
    select *, datediff(current_date(), reading_ts) / 30 as months_since_reading_ts
    from device_readings
    where reading_ts >= date '2025-09-01'
),
prev_events as (
    select *, lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) as prev_reading_ts
    from filtered
),
with_gaps as (
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '45' minute then 1 else 0 end as is_new_burst
    from prev_events
),
sessionized as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        sum(is_new_burst) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as session_no
    from with_gaps
),
with_gaps_2 as (
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) > interval '20' minute then 1 else 0 end as is_new_session
    from sessionized
),
sessionized_device_readings as (
    select *, sum(is_new_session) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as island_id
    from with_gaps_2
),
with_windows as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        session_no,
        is_new_session,
        island_id,
        max(temp_c) over w_part as running_max_temp_c,
        min(temp_c) over w_part as running_min_temp_c,
        count(temp_c) over w_part as running_count_temp_c,
        sum(temp_c) over w_part as running_sum_temp_c
    from sessionized_device_readings
    where firmware = '1.1.0'
    window w_part as (partition by device_code order by session_no asc, id asc rows between unbounded preceding and current row)
),
base as (
    select
        id,
        is_new_session,
        running_min_temp_c,
        session_no - island_id as session_no_minus_island_id,
        ceil(session_no) as session_no_whole
    from with_windows
    where running_max_temp_c between 31 and 36
),
valid as (
    select
        *,
        round(is_new_session, 2) as is_new_session_rounded,
        session_no_whole - running_min_temp_c as session_no_whole_minus_running_min_temp_c
    from base
    where is_new_session != 0
)
select
    case when session_no_whole >= 87 then 'HIGH' else 'LOW' end as session_no_whole_tier,
    case when running_min_temp_c >= 38.2 then 'LARGE' else 'SMALL' end as running_min_temp_c_tier,
    case when is_new_session_rounded >= 73 then 'PREMIUM' else 'STANDARD' end as is_new_session_rounded_tier,
    approx_percentile(is_new_session_rounded, array(0.25, 0.5, 0.75)) as is_new_session_rounded_quartiles,
    max(is_new_session) as max_is_new_session,
    min(running_min_temp_c) as min_running_min_temp_c,
    count(*) as num_rows
from valid
group by case when session_no_whole >= 87 then 'HIGH' else 'LOW' end, case when running_min_temp_c >= 38.2 then 'LARGE' else 'SMALL' end, case when is_new_session_rounded >= 73 then 'PREMIUM' else 'STANDARD' end;
