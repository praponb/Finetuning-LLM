with cleaned as (
    select
        id,
        device_code,
        reading_ts,
        humidity,
        temp_c / nullif(status_bits, 0) as temp_c_per_status_bits
    from device_readings
    where hour(reading_ts) between 11 and 16
),
with_windows as (
    select
        id,
        device_code,
        humidity,
        temp_c_per_status_bits,
        count(humidity) over (partition by device_code) as device_code_count_humidity,
        cume_dist() over (partition by month(reading_ts), device_code order by temp_c_per_status_bits desc nulls last) as temp_c_per_status_bits_cume_dist,
        last_value(humidity) ignore nulls over (order by humidity, id rows between unbounded preceding and current row) as last_known_humidity,
        sum(humidity) over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as cumulative_humidity,
        last_value(humidity) ignore nulls over (partition by year(reading_ts), device_code order by humidity, id rows between unbounded preceding and current row) as last_known_humidity_2
    from cleaned
),
latest as (
    select
        id,
        device_code,
        last_known_humidity,
        cumulative_humidity
    from (
        select *, row_number() over (partition by device_code order by last_known_humidity_2 desc, id) as rn
        from with_windows
    ) ranked
    where rn = 1
),
base as (
    select *, last_known_humidity - cumulative_humidity as last_known_humidity_minus_cumulative_humidity
    from latest
    where last_known_humidity <= 68
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by device_code order by cumulative_humidity desc, id) as rank_in_group
        from base
    ) x
    where rank_in_group = 1
)
select
    id,
    device_code,
    last_known_humidity,
    cumulative_humidity,
    last_known_humidity_minus_cumulative_humidity,
    rank_in_group,
    avg(last_known_humidity_minus_cumulative_humidity) over (partition by device_code order by last_known_humidity_minus_cumulative_humidity, id rows between unbounded preceding and current row) as running_avg_last_known_humidity_minus_cumulative_humidity,
    count(last_known_humidity_minus_cumulative_humidity) over (partition by device_code order by last_known_humidity_minus_cumulative_humidity, id rows between unbounded preceding and current row) as running_count_last_known_humidity_minus_cumulative_humidity,
    min(last_known_humidity_minus_cumulative_humidity) over (partition by device_code order by last_known_humidity_minus_cumulative_humidity, id rows between unbounded preceding and current row) as running_min_last_known_humidity_minus_cumulative_humidity
from ranked;
