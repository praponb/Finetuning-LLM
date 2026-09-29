with valid as (
    -- Filter the raw rows and derive helper columns
    select *, temp_c - pressure as temp_c_minus_pressure
    from device_readings
    where firmware != '1.0.3'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        reading_ts,
        temp_c,
        pressure,
        lag(status_bits, 1) over (partition by month(reading_ts), device_code order by reading_ts, id) as prev_status_bits,
        row_number() over w_recent as event_seq
    from valid
    where temp_c >= 24
    window w_recent as (partition by device_code order by humidity desc, id)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, cast(temp_c as int) as temp_c_int
    from with_windows
    where reading_ts >= timestamp '2025-09-01 01:00:00'
),
with_windows_device_readings as (
    -- Running and moving aggregates
    select
        id,
        reading_ts,
        temp_c,
        pressure,
        temp_c_int,
        lag(prev_status_bits) over (order by reading_ts, id) as prev_prev_status_bits,
        lead(event_seq) over (order by reading_ts, id) as next_event_seq,
        lag(event_seq) over (order by reading_ts, id) as prev_event_seq,
        row_number() over (order by reading_ts, id) as event_seq_2
    from cleaned
)
-- Grouped metrics
select
    date_trunc('day', reading_ts) as activity_day,
    date_trunc('hour', reading_ts) as hour_window,
    min(reading_ts) as first_seen,
    max(temp_c_int) as max_temp_c_int,
    sum(pressure) as total_pressure
from with_windows_device_readings
group by date_trunc('day', reading_ts), date_trunc('hour', reading_ts)
having total_pressure > 0;
