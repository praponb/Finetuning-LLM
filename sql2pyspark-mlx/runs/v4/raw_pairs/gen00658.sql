with enriched as (
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
        corr(status_bits, humidity) over (partition by device_code) as corr_status_bits_humidity,
        lag(pressure, 1) over (partition by device_code order by reading_ts, id) as prev_pressure,
        sum(humidity) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as cumulative_humidity
    from device_readings
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        greatest(pressure, status_bits) as greatest_pressure_status_bits,
        prev_pressure - pressure as prev_pressure_minus_pressure
    from enriched
    where reading_ts >= to_date('2025-09-01')
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by device_code order by humidity desc nulls last, id) as latest_rank
        from base
    ) ranked
    where latest_rank = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        corr_status_bits_humidity,
        prev_pressure,
        cumulative_humidity,
        greatest_pressure_status_bits,
        prev_pressure_minus_pressure,
        corr_status_bits_humidity - prev_pressure_minus_pressure as corr_status_bits_humidity_minus_prev_pressure_minus_pressure
    from latest
    where reading_ts >= date '2025-09-01'
)
-- Aggregate with conditional metrics
select
    case when status_bits >= 11 then 'PREMIUM' else 'STANDARD' end as status_bits_tier,
    case when corr_status_bits_humidity_minus_prev_pressure_minus_pressure >= 63 then 'HIGH' else 'LOW' end as corr_status_bits_humidity_minus_prev_pressure_minus_pressure_tier,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes,
    percentile_approx(prev_pressure_minus_pressure, 0.5) as prev_pressure_minus_pressure_p50
from valid
group by status_bits_tier, corr_status_bits_humidity_minus_prev_pressure_minus_pressure_tier
order by status_bits_tier desc nulls first;
