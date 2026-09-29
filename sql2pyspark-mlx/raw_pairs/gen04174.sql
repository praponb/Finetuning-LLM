with filtered as (
    -- Keep recent, valid rows
    select
        id,
        reading_ts,
        humidity,
        pressure,
        firmware,
        floor(temp_c) temp_c_whole
    from device_readings
    where reading_ts >= '2024-05-17'
),
scored as (
    -- Ranking and gap features
    select
        *,
        ntile(10) over (order by humidity, id) humidity_bucket,
        lead(pressure, 1) over (partition by month(reading_ts) order by reading_ts, id) next_pressure,
        sum(temp_c_whole) over (order by cast(reading_ts as bigint) range between 1800 preceding and current row) temp_c_whole_sum_30m,
        ntile(3) over (order by pressure, id) pressure_bucket,
        ntile(5) over (order by temp_c_whole desc, id) temp_c_whole_bucket
    from filtered
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        last_value(temp_c_whole, true) over (order by pressure, id rows between unbounded preceding and current row) last_known_temp_c_whole,
        avg(humidity) over (order by humidity_bucket, id rows between 5 preceding and current row) humidity_avg_5
    from scored
    where to_date(reading_ts) >= '2025-03-22'
),
filtered_device_readings as (
    -- Keep recent, valid rows
    select *, coalesce(humidity_bucket, 0) humidity_bucket_filled
    from enriched
    where last_known_temp_c_whole between 37 and 68
),
summary as (
    -- Grouped metrics
    select
        case when temp_c_whole_bucket >= 7 then 'PREMIUM' else 'STANDARD' end as temp_c_whole_bucket_tier,
        max(reading_ts) as last_reading_ts,
        min(reading_ts) as started_at
    from filtered_device_readings
    group by case when temp_c_whole_bucket >= 7 then 'PREMIUM' else 'STANDARD' end
)
select
    temp_c_whole_bucket_tier,
    last_reading_ts,
    started_at
from summary
