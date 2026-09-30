with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by device_code order by temp_c asc, id asc) as row_num
        from device_readings
    ) ranked
    where row_num = 1
),
prev_events as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) as prev_reading_ts
    from deduped
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '30' minute then 1 else 0 end as new_group_flag
    from prev_events
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by device_code order by temp_c asc, id asc) as row_seq,
        sum(temp_c) over (partition by device_code order by humidity asc, id asc rows between unbounded preceding and current row) as running_temp_c
    from sessions_device_readings
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(humidity) over w_recent as cumulative_humidity,
        avg(burst_id) over (partition by device_code order by running_temp_c asc, id asc rows between 2 preceding and 1 preceding) as burst_id_avg_2_prior,
        lag(row_seq, 1) over (order by temp_c asc, id asc) as prev_row_seq,
        sum(new_group_flag) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as running_new_group_flag
    from with_windows
    window w_recent as (partition by device_code order by new_group_flag asc, id asc rows between unbounded preceding and current row)
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        burst_id,
        row_seq,
        cumulative_humidity,
        burst_id_avg_2_prior,
        prev_row_seq,
        running_new_group_flag,
        case when prev_row_seq >= 8 then 'High' when prev_row_seq >= 4 then 'Medium' else 'Low' end as prev_row_seq_level
    from enriched
    where new_group_flag between 0 and 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, cast(reading_ts as date) as reading_ts_day
    from cleaned
    where reading_ts >= date '2025-09-01'
      and device_code in ('DV-02', 'DV-01', 'DV-04')
)
select
    id,
    device_code,
    reading_ts,
    firmware,
    burst_id,
    row_seq,
    burst_id_avg_2_prior,
    prev_row_seq,
    prev_row_seq_level
from filtered;
