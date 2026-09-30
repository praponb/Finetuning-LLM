with with_windows as (
    -- Running and moving aggregates
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        ntile(10) over (order by pressure desc, id) as pressure_bucket,
        row_number() over (partition by device_code order by status_bits, id) as row_seq,
        avg(humidity) over (order by reading_ts, id rows between 3 preceding and current row) as humidity_avg_3
    from device_readings
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        pressure_bucket,
        row_seq,
        humidity_avg_3,
        round(humidity, 1) as humidity_rounded
    from with_windows
    where reading_ts > timestamp '2025-09-01 03:00:00'
),
valid as (
    -- Keep recent, valid rows
    select *, ceil(humidity) as humidity_whole
    from cleaned
    where reading_ts > date '2025-09-01'
),
valid_device_readings as (
    -- Basic cleansing and defaults
    select
        *,
        case when pressure >= 1019 then 'High' when pressure >= 999 then 'Medium' else 'Low' end as pressure_tier,
        round(row_seq / humidity, 2) as row_seq_per_humidity
    from valid
    where pressure_bucket between 7 and 8
)
select
    id,
    reading_ts,
    humidity,
    pressure,
    row_seq,
    humidity_rounded,
    humidity_whole,
    pressure_tier
from valid_device_readings
