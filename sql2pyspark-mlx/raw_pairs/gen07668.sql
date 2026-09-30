with valid as (
    -- Keep recent, valid rows
    select
        *,
        temp_c + humidity as temp_c_plus_humidity,
        coalesce(temp_c, 0) as temp_c_filled
    from device_readings
    where reading_ts > to_timestamp('2025-09-01 08:00:00')
      and humidity <= 64
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '45' minute then 1 else 0 end as session_start_flag
    from valid
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        temp_c_plus_humidity,
        sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
)
-- Grouped metrics
select
    case when temp_c_plus_humidity >= 82 then 'PREMIUM' else 'STANDARD' end as temp_c_plus_humidity_tier,
    max(reading_ts) as last_seen,
    count(distinct id) as distinct_ids,
    size(collect_set(device_code)) as num_device_codes
from islands
group by 1
order by distinct_ids nulls last
