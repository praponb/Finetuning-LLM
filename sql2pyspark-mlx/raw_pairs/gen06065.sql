with gaps as (
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '45' minute then 1 else 0 end as new_group_flag
    from device_readings
),
sessionized as (
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
ranked as (
    select *, row_number() over (partition by device_code order by new_group_flag nulls last, id) as rn
    from sessionized
    qualify rn = 1
),
deduped as (
    select *
    from ranked
    qualify row_number() over (partition by device_code order by burst_id desc, id) = 1
),
metrics as (
    select
        *,
        sum(humidity) over (partition by device_code) as device_code_sum_humidity,
        max(pressure) over w_grp as pressure_max_2h,
        sum(burst_id) over w_running as running_burst_id
    from deduped
    window w_grp as (partition by device_code order by cast(reading_ts as bigint) range between 7200 preceding and current row), w_running as (order by reading_ts, id rows between unbounded preceding and current row)
),
base as (
    select *, pressure + temp_c as pressure_plus_temp_c
    from metrics
    where reading_ts >= timestamp '2025-09-01 02:00:00'
),
totals as (
    select
        device_code,
        date_trunc('day', reading_ts) as activity_day,
        count(*) filter (where device_code = 'DV-01') as dv_01_rows,
        count(*) as cnt,
        count(case when device_code = 'DV-02' then 1 end) as dv_02_count
    from base
    group by device_code, date_trunc('day', reading_ts)
)
select
    device_code,
    activity_day,
    dv_01_rows
from totals;
