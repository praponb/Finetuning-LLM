with with_windows as (
    -- Running and moving aggregates
    select
        *,
        count(temp_c) over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as running_count_temp_c,
        sum(temp_c) over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as running_sum_temp_c,
        min(temp_c) over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as running_min_temp_c,
        ntile(10) over (partition by year(reading_ts) order by pressure desc, id) as pressure_bucket
    from device_readings
),
enriched as (
    -- Ranking and gap features
    select
        id,
        device_code,
        temp_c,
        firmware,
        running_count_temp_c,
        round(humidity / sum(humidity) over (partition by device_code), 4) as humidity_share,
        ntile(3) over (partition by device_code order by running_sum_temp_c desc, id) as running_sum_temp_c_bucket,
        round(pressure / sum(pressure) over (partition by device_code), 4) as pressure_share,
        sum(humidity) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as cumulative_humidity,
        sum(running_min_temp_c) over (partition by device_code order by running_min_temp_c, id rows between unbounded preceding and current row) as running_running_min_temp_c
    from with_windows
    where pressure <= 1013
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by humidity_share desc nulls first, id) as rank_in_group
    from enriched
    qualify rank_in_group = 1
),
latest_device_readings as (
    -- Keep one row per key (latest first)
    select *
    from latest
    qualify row_number() over (partition by device_code order by rank_in_group desc, id) = 1
),
with_windows_device_readings as (
    -- Ranking and gap features
    select
        *,
        sum(temp_c) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_sum_temp_c,
        min(temp_c) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_min_temp_c,
        max(temp_c) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_max_temp_c,
        ntile(5) over (partition by device_code order by running_sum_temp_c_bucket, id) as running_sum_temp_c_bucket_bucket
    from latest_device_readings
),
latest_device_readings_2 as (
    -- QUALIFY-style window filter
    select *
    from with_windows_device_readings
    qualify row_number() over (partition by device_code order by running_min_temp_c nulls last, id) = 1
)
select
    id,
    device_code,
    temp_c,
    firmware,
    running_sum_temp_c_bucket,
    pressure_share,
    running_running_min_temp_c,
    rank_in_group,
    running_min_temp_c
from latest_device_readings_2
order by temp_c nulls first, device_code desc
