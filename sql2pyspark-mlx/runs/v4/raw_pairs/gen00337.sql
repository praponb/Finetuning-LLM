with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 1 hour then 1 else 0 end as is_new_burst
    from device_readings
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
remaining_keys as (
    -- Set intersection on keys
    select firmware, id
    from islands
    intersect
    select firmware, id
    from device_readings
    where humidity > 63
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from islands s
    join remaining_keys k on s.firmware = k.firmware and s.id = k.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        is_new_burst,
        burst_id,
        max(burst_id) over (partition by device_code) as device_code_max_burst_id,
        max(temp_c) over (partition by device_code) as device_code_max_temp_c
    from kept_rows
),
metrics_device_readings as (
    -- Running and moving aggregates
    select
        *,
        count(pressure) over (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row) as pressure_count_15m,
        count(pressure) over w_part as device_code_count_pressure
    from metrics
    window w_part as (partition by device_code)
)
select
    reading_ts,
    humidity,
    status_bits,
    firmware,
    burst_id,
    device_code_max_burst_id,
    device_code_count_pressure
from metrics_device_readings;
