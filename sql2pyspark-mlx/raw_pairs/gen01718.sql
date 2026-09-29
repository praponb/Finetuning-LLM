with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        round(status_bits * 1.07, 2) status_bits_scaled,
        greatest(pressure, status_bits) greatest_pressure_status_bits
    from device_readings
    where status_bits <> 3
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by device_code order by temp_c nulls first) as latest_rank
    from valid
    qualify latest_rank = 1
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits_scaled,
        greatest_pressure_status_bits,
        latest_rank
    from (
        select *, row_number() over (partition by device_code order by latest_rank desc, id) as rank_in_group
        from deduped
    ) x
    where rank_in_group = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '15' minute then 1 else 0 end as session_start_flag
    from latest
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
)
-- Multi-level aggregation
select
    case when pressure >= 1025 then 'HIGH' else 'LOW' end as pressure_tier,
    device_code,
    sort_array(collect_set(device_code)) as device_code_list,
    size(collect_set(device_code)) as num_device_codes,
    percent_rank() over (partition by case when pressure >= 1025 then 'HIGH' else 'LOW' end order by size(collect_set(device_code)) desc) as num_device_codes_percent_rank
from islands
group by case when pressure >= 1025 then 'HIGH' else 'LOW' end, device_code
order by pressure_tier nulls first
