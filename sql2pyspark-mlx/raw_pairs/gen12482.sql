with valid as (
    -- Keep recent, valid rows
    select
        *,
        length(firmware) as firmware_length,
        lower(device_code) as device_code_lower
    from device_readings
    where temp_c <= 33
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        pressure,
        firmware,
        firmware_length,
        device_code_lower,
        cast(temp_c as int) as temp_c_int
    from valid
    where humidity >= 74
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by device_code order by reading_ts desc, id) as row_num
        from cleaned
    ) t
    where row_num = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        pressure,
        firmware,
        firmware_length,
        device_code_lower,
        max(pressure) over w_part as running_max_pressure,
        count(pressure) over w_part as running_count_pressure,
        avg(pressure) over w_part as running_avg_pressure,
        count(firmware_length) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) as firmware_length_count_30m
    from latest
    window w_part as (partition by device_code order by temp_c_int, id rows between unbounded preceding and current row)
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by device_code order by running_max_pressure desc, id) as dedup_rank
    from metrics
    qualify dedup_rank = 1
)
-- Ranking and gap features
select
    id,
    device_code,
    pressure,
    firmware,
    firmware_length,
    device_code_lower,
    running_max_pressure,
    running_count_pressure,
    running_avg_pressure,
    firmware_length_count_30m,
    dedup_rank,
    lead(running_count_pressure, 1) over (partition by device_code order by running_count_pressure, id) as next_running_count_pressure,
    round(running_max_pressure / sum(running_max_pressure) over (partition by device_code), 4) as running_max_pressure_share,
    round(running_count_pressure / sum(running_count_pressure) over (partition by device_code), 4) as running_count_pressure_share,
    row_number() over (partition by device_code order by dedup_rank, id) as event_seq,
    ntile(4) over (partition by device_code order by running_max_pressure, id) as running_max_pressure_bucket
from ranked
