with cleaned as (
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        round(temp_c, 0) as temp_c_rounded,
        round(pressure * 1.07, 2) as adjusted_pressure
    from device_readings
    where reading_ts >= timestamp '2025-09-01 06:00:00'
),
scored as (
    select
        id,
        reading_ts,
        humidity,
        pressure,
        adjusted_pressure,
        first_value(pressure) ignore nulls over (order by temp_c_rounded, id rows between unbounded preceding and current row) as first_known_pressure,
        last_value(pressure) ignore nulls over w_part as last_known_pressure,
        ntile(4) over w_running as status_bits_bucket
    from cleaned
    where adjusted_pressure between 22 and 48
    window w_part as (partition by year(reading_ts) order by temp_c_rounded, id rows between unbounded preceding and current row), w_running as (order by status_bits, id)
),
filtered as (
    select
        id,
        reading_ts,
        humidity,
        pressure,
        adjusted_pressure,
        status_bits_bucket,
        coalesce(last_known_pressure, 0) as last_known_pressure_filled,
        greatest(first_known_pressure, last_known_pressure) as greatest_first_known_pressure_last_known_pressure
    from scored
    where reading_ts > to_date('2025-09-01')
),
with_windows as (
    select
        *,
        stddev_samp(humidity) over (partition by year(reading_ts)) as reading_ts_year_stddev_samp_humidity,
        avg(adjusted_pressure) over (order by greatest_first_known_pressure_last_known_pressure, id rows between 2 preceding and current row) as adjusted_pressure_avg_2,
        lead(status_bits_bucket, 2, 0) over (order by humidity, id) as next_status_bits_bucket
    from filtered
),
scored_device_readings as (
    select
        id,
        humidity,
        adjusted_pressure,
        status_bits_bucket,
        last_known_pressure_filled,
        greatest_first_known_pressure_last_known_pressure,
        reading_ts_year_stddev_samp_humidity,
        adjusted_pressure_avg_2,
        lag(last_known_pressure_filled) over w_part as prev_last_known_pressure_filled,
        lead(last_known_pressure_filled) over w_part as next_last_known_pressure_filled,
        lag(status_bits_bucket) over w_part as prev_status_bits_bucket,
        lead(status_bits_bucket) over w_part as next_status_bits_bucket_2,
        row_number() over w_part as row_seq
    from with_windows
    window w_part as (order by adjusted_pressure, id)
)
select
    id,
    humidity,
    adjusted_pressure,
    status_bits_bucket,
    reading_ts_year_stddev_samp_humidity,
    adjusted_pressure_avg_2,
    prev_last_known_pressure_filled,
    next_last_known_pressure_filled,
    next_status_bits_bucket_2
from scored_device_readings
order by next_last_known_pressure_filled nulls last
