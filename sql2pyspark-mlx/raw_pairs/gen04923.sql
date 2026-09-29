with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(status_bits * 100, 2) as adjusted_status_bits,
        substring(firmware, 1, 2) as firmware_prefix
    from device_readings
    where reading_ts >= to_date('2025-09-01')
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '1' hour then 1 else 0 end as is_new_burst
    from filtered
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from flagged
)
-- Multi-level aggregation
select
    case when humidity >= 67 then 'HIGH' else 'LOW' end as humidity_tier,
    date_trunc('hour', reading_ts) as hour_window,
    case when pressure >= 1023 then 'HIGH' else 'LOW' end as pressure_tier,
    count_if(device_code = 'DV-03') as dv_03_events,
    sort_array(collect_set(device_code)) as device_code_list
from islands
group by case when humidity >= 67 then 'HIGH' else 'LOW' end, date_trunc('hour', reading_ts), case when pressure >= 1023 then 'HIGH' else 'LOW' end
having dv_03_events < 1000;
