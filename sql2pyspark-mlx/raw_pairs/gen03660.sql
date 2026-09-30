with filtered as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        case when pressure >= 1030 then 'High' when pressure >= 1001 then 'Medium' else 'Low' end as pressure_band
    from device_readings
    where pressure between 1001 and 1027
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        cast(reading_ts as date) as reading_ts_day,
        datediff(current_date(), reading_ts) / 365 as years_since_reading_ts
    from filtered
    where reading_ts > timestamp '2025-09-01 08:00:00'
),
filtered_device_readings as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        pressure - status_bits as pressure_minus_status_bits,
        humidity + status_bits as humidity_plus_status_bits
    from cleaned
    where firmware = '2.0.1'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        temp_c,
        pressure,
        status_bits,
        years_since_reading_ts,
        humidity_plus_status_bits,
        row_number() over (partition by device_code order by humidity_plus_status_bits, id) as event_seq,
        lag(pressure_minus_status_bits, 1) over (partition by device_code order by temp_c, id) as prev_pressure_minus_status_bits,
        lead(humidity_plus_status_bits, 1, 0) over w_hist as next_humidity_plus_status_bits
    from filtered_device_readings
    window w_hist as (partition by device_code order by status_bits, id)
)
-- Multi-level aggregation
select
    case when prev_pressure_minus_status_bits >= 90 then 'PREMIUM' else 'STANDARD' end as prev_pressure_minus_status_bits_tier,
    case when next_humidity_plus_status_bits >= 73 then 'HIGH' else 'LOW' end as next_humidity_plus_status_bits_tier,
    sum(event_seq) as total_event_seq,
    round(avg(years_since_reading_ts), 2) as avg_years_since_reading_ts,
    rank() over (partition by case when prev_pressure_minus_status_bits >= 90 then 'PREMIUM' else 'STANDARD' end order by round(avg(years_since_reading_ts), 2) desc) as avg_years_since_reading_ts_rank
from with_windows
group by prev_pressure_minus_status_bits_tier, next_humidity_plus_status_bits_tier
having sum(event_seq) >= 0
order by total_event_seq desc
