with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(humidity, 0) as humidity_rounded,
        round(status_bits, 0) as status_bits_rounded
    from device_readings
    where reading_ts > timestamp '2025-09-01 09:00:00'
      and pressure <= 1014
),
scored as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by device_code order by reading_ts asc, id asc) as row_seq,
        round(humidity / sum(humidity) over (partition by year(reading_ts), device_code), 4) as humidity_share,
        round(humidity / sum(humidity) over (partition by device_code), 4) as humidity_share_2,
        row_number() over w_grp as event_seq,
        lag(status_bits, 2) over w_part as prev_status_bits
    from valid
    where status_bits_rounded between 24 and 67
    window w_grp as (partition by device_code order by status_bits_rounded asc, id asc), w_part as (partition by month(reading_ts), device_code order by temp_c asc, id asc)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        min(humidity_share_2) over (partition by device_code) as device_code_min_humidity_share_2,
        lead(temp_c, 2) over (partition by device_code order by humidity_share asc, id asc) as next_temp_c,
        ntile(4) over (order by humidity_share asc, id asc) as humidity_share_bucket,
        percent_rank() over (partition by month(reading_ts), device_code order by humidity desc) as humidity_percent_rank,
        row_number() over (partition by year(reading_ts), device_code order by prev_status_bits desc, id asc) as event_seq_2
    from scored
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        humidity,
        pressure,
        status_bits_rounded,
        prev_status_bits,
        device_code_min_humidity_share_2,
        next_temp_c,
        humidity_percent_rank,
        event_seq_2,
        min(humidity_rounded) over w_grp as running_min_humidity_rounded,
        count(humidity_rounded) over w_grp as running_count_humidity_rounded,
        avg(humidity_rounded) over w_grp as running_avg_humidity_rounded,
        max(humidity_rounded) over w_grp as running_max_humidity_rounded
    from metrics
    window w_grp as (partition by device_code order by row_seq asc, id asc rows between unbounded preceding and current row)
)
-- Aggregate with conditional metrics
select
    case when status_bits_rounded >= 82 then 'LARGE' else 'SMALL' end as status_bits_rounded_tier,
    approx_percentile(event_seq_2, array(0.25, 0.5, 0.75)) as event_seq_2_quartiles,
    approx_percentile(humidity, 0.5) as humidity_p50,
    count(*) as row_count,
    count(distinct id) as distinct_ids
from with_windows
group by status_bits_rounded_tier
order by row_count desc;
