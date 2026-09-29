with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, round(temp_c * 0.9, 2) as temp_c_scaled
    from device_readings
    where reading_ts < '2024-09-13'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        temp_c,
        humidity,
        temp_c_scaled,
        sum(temp_c) over w_grp as running_temp_c,
        round(pressure / sum(pressure) over (partition by device_code), 4) as pressure_share
    from cleaned
    window w_grp as (partition by device_code order by reading_ts, id rows between unbounded preceding and current row)
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        temp_c,
        pressure_share,
        lag(running_temp_c) over w_ordered as prev_running_temp_c,
        lead(humidity) over w_ordered as next_humidity,
        lag(humidity) over w_ordered as prev_humidity,
        row_number() over w_ordered as event_seq,
        lag(pressure_share, 1) over w_ordered as prev_pressure_share
    from with_windows
    window w_ordered as (order by humidity, id)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        temp_c,
        pressure_share,
        prev_running_temp_c,
        next_humidity,
        prev_humidity,
        prev_pressure_share,
        floor(prev_pressure_share) as prev_pressure_share_whole
    from metrics
    where next_humidity between 54 and 74
)
select
    id,
    temp_c,
    pressure_share,
    prev_running_temp_c,
    prev_pressure_share,
    prev_pressure_share_whole
from filtered
order by prev_running_temp_c
