with with_prev as (
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from device_readings
),
gaps as (
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 15 minutes then 1 else 0 end as new_group_flag
    from with_prev
)
select
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    status_bits,
    sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
from gaps;
