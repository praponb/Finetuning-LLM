with base as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        round(humidity * 1.15, 2) as humidity_adjusted,
        round(humidity * 1.07, 2) as adjusted_humidity
    from device_readings
    where reading_ts >= to_timestamp('2025-09-01 05:00:00')
),
enriched as (
    -- Ranking and gap features
    select
        id,
        firmware,
        humidity_adjusted,
        adjusted_humidity,
        ntile(3) over (order by humidity, id) as humidity_bucket,
        avg(temp_c) over w_running as temp_c_avg_30m
    from base
    where adjusted_humidity < 51
    window w_running as (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        firmware,
        humidity_adjusted,
        adjusted_humidity,
        humidity_bucket,
        coalesce(humidity_bucket, 0) as humidity_bucket_filled
    from enriched
    where humidity_bucket between 3 and 7
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(humidity_adjusted, true) over (order by humidity_bucket, id rows between unbounded preceding and current row) as first_known_humidity_adjusted,
        lead(adjusted_humidity, 1) over (order by humidity_bucket, id) as next_adjusted_humidity,
        rank() over (order by humidity_bucket desc nulls first) as humidity_bucket_rank,
        sum(humidity_bucket_filled) over (order by humidity_bucket_filled, id rows between unbounded preceding and current row) as running_humidity_bucket_filled
    from valid
),
valid_device_readings as (
    -- Filter the raw rows and derive helper columns
    select *, case when firmware in ('1.0.3', '2.0.1') then 'Priority' else 'Other' end as firmware_group
    from metrics
    where next_adjusted_humidity < 20
),
filtered as (
    -- Basic cleansing and defaults
    select *, round(humidity_bucket_rank / humidity_bucket, 2) as humidity_bucket_rank_per_humidity_bucket
    from valid_device_readings
    where next_adjusted_humidity > 74
)
-- Grouped metrics
select
    case when running_humidity_bucket_filled >= 381 then 'HIGH' else 'LOW' end as running_humidity_bucket_filled_tier,
    round(avg(adjusted_humidity), 2) as avg_adjusted_humidity,
    round(avg(humidity_bucket_rank_per_humidity_bucket), 2) as avg_humidity_bucket_rank_per_humidity_bucket,
    count(*) as num_rows
from filtered
group by case when running_humidity_bucket_filled >= 381 then 'HIGH' else 'LOW' end;
