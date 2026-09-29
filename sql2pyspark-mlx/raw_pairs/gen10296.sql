with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '15' minute then 1 else 0 end as is_new_burst
    from device_readings
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from gaps
),
latest as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by device_code order by reading_ts desc) as row_num
    from islands
    qualify row_num = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from latest
    qualify row_number() over (partition by device_code order by row_num, id) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        row_num,
        count(pressure) over (partition by device_code order by session_id, id rows between 2 preceding and current row) as last3_count_pressure,
        max(pressure) over (partition by device_code order by session_id, id rows between 2 preceding and current row) as last3_max_pressure,
        avg(pressure) over (partition by device_code order by session_id, id rows between 2 preceding and current row) as last3_avg_pressure,
        sum(pressure) over (partition by device_code order by session_id, id rows between 2 preceding and current row) as last3_sum_pressure
    from deduped
    where firmware not in ('1.0.3', '2.0.1')
)
select
    id,
    row_num,
    last3_count_pressure,
    last3_max_pressure,
    last3_avg_pressure,
    last3_sum_pressure
from metrics
order by row_num nulls last;
