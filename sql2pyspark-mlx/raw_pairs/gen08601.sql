with valid as (
    select
        *,
        cast(pressure as int) as pressure_int,
        datediff(current_date(), reading_ts) as days_since_reading_ts
    from device_readings
    where reading_ts > to_timestamp('2025-09-01 06:00:00')
),
scored as (
    select
        *,
        lag(humidity) over w_part as prev_humidity,
        lead(humidity) over w_part as next_humidity,
        lead(status_bits) over w_part as next_status_bits,
        lag(status_bits) over w_part as prev_status_bits,
        row_number() over w_part as seq_no,
        sum(pressure) over w_recent as running_pressure
    from valid
    window w_part as (partition by device_code order by humidity, id), w_recent as (partition by year(reading_ts), device_code order by pressure, id rows between unbounded preceding and current row)
),
ranked as (
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        pressure_int,
        next_status_bits,
        prev_status_bits,
        seq_no
    from (
        select *, rank() over (partition by device_code order by prev_status_bits desc nulls last) as rank_in_group
        from scored
    ) t
    where rank_in_group = 1
),
base as (
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        pressure_int,
        next_status_bits,
        prev_status_bits,
        seq_no,
        date_format(reading_ts, 'yyyy-MM-dd') as reading_ts_label
    from ranked
    where reading_ts > timestamp '2025-09-01 04:00:00'
),
filtered as (
    select
        id,
        device_code,
        reading_ts,
        pressure,
        next_status_bits,
        seq_no,
        reading_ts_label,
        round(prev_status_bits * 1.15, 2) as adjusted_prev_status_bits
    from base
    where reading_ts > to_timestamp('2025-09-01 05:00:00')
),
with_windows as (
    select
        id,
        next_status_bits,
        reading_ts_label,
        adjusted_prev_status_bits,
        avg(pressure) over (partition by device_code order by next_status_bits, id rows between unbounded preceding and current row) as running_avg_pressure,
        min(pressure) over (partition by device_code order by next_status_bits, id rows between unbounded preceding and current row) as running_min_pressure,
        max(pressure) over (partition by device_code order by next_status_bits, id rows between unbounded preceding and current row) as running_max_pressure
    from filtered
)
select
    *,
    avg(running_max_pressure) over (order by adjusted_prev_status_bits, id rows between 2 preceding and current row) as running_max_pressure_avg_2,
    sum(running_avg_pressure) over (order by running_max_pressure, id rows between unbounded preceding and current row) as cumulative_running_avg_pressure,
    row_number() over (order by next_status_bits desc, id) as event_seq
from with_windows
