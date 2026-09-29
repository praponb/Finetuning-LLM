with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 1 hour then 1 else 0 end as new_group_flag
    from device_readings
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
select
    id,
    device_code,
    humidity,
    status_bits,
    firmware,
    case when device_code rlike '^(DV-02|DV-01)$' then 'CORE' when device_code rlike '^(DV-03|DV-04)$' then 'OTHER_ZONE' else 'GROUP_A' end as device_code_zone
from islands
order by firmware desc
