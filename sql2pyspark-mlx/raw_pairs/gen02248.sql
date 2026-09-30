with valid as (
    select *, round(humidity, 2) as humidity_rounded
    from device_readings
    where reading_ts >= timestamp '2025-09-01 08:00:00'
      and status_bits >= 6
),
valid_device_readings as (
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        humidity_rounded,
        cast(humidity as int) as humidity_int
    from valid
    where temp_c < 32
),
with_windows as (
    select
        *,
        max(temp_c) over (order by temp_c, id rows between unbounded preceding and current row) as running_max_temp_c,
        avg(temp_c) over (order by temp_c, id rows between unbounded preceding and current row) as running_avg_temp_c,
        min(temp_c) over (order by temp_c, id rows between unbounded preceding and current row) as running_min_temp_c,
        cume_dist() over (order by pressure nulls last) as pressure_cume_dist
    from valid_device_readings
),
with_windows_device_readings as (
    select
        id,
        reading_ts,
        temp_c,
        running_avg_temp_c,
        running_min_temp_c,
        ntile(5) over (order by humidity_int desc, id) as humidity_int_bucket,
        ntile(10) over (partition by year(reading_ts) order by running_max_temp_c, id) as running_max_temp_c_bucket,
        avg(humidity_int) over w_grp as humidity_int_avg_2_prior,
        ntile(3) over (order by running_avg_temp_c, id) as running_avg_temp_c_bucket
    from with_windows
    where humidity_int != 46
    window w_grp as (order by pressure, id rows between 2 preceding and 1 preceding)
),
valid_device_readings_2 as (
    select
        id,
        reading_ts,
        running_avg_temp_c,
        running_min_temp_c,
        humidity_int_bucket,
        humidity_int_avg_2_prior,
        running_avg_temp_c_bucket,
        running_min_temp_c * running_avg_temp_c as running_min_temp_c_times_running_avg_temp_c,
        running_min_temp_c + running_avg_temp_c as running_min_temp_c_plus_running_avg_temp_c
    from with_windows_device_readings
    where hour(reading_ts) between 10 and 17
),
base as (
    select *, datediff(current_date(), reading_ts) / 30 as months_since_reading_ts
    from valid_device_readings_2
    where humidity_int_bucket < 3
)
select
    id,
    reading_ts,
    running_min_temp_c,
    humidity_int_avg_2_prior,
    running_avg_temp_c_bucket,
    running_min_temp_c_times_running_avg_temp_c,
    running_min_temp_c_plus_running_avg_temp_c,
    months_since_reading_ts
from base
order by humidity_int_avg_2_prior
