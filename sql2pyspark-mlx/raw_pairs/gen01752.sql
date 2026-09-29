with gaps as (
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '30' minute then 1 else 0 end as is_new_session
    from device_readings
)
select
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    firmware,
    sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
from gaps;
