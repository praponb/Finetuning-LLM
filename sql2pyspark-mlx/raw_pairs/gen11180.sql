with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        case when pressure >= 1032 then 'High' when pressure >= 987 then 'Medium' else 'Low' end as pressure_tier,
        round(status_bits, 2) as status_bits_rounded
    from device_readings
    where device_code = 'DV-02'
),
remaining_keys as (
    -- Set difference on keys
    select device_code, id
    from base
    except all
    select device_code, id
    from device_readings
    where humidity > 64
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.device_code = k.device_code and s.id = k.id
),
scored as (
    -- Ranking and gap features
    select *, round(humidity / sum(humidity) over (partition by device_code), 4) as humidity_share
    from verified
),
scored_device_readings as (
    -- Ranking and gap features
    select
        reading_ts,
        humidity,
        status_bits_rounded,
        humidity_share,
        dense_rank() over w_grp as humidity_share_dense_rank,
        rank() over (partition by device_code order by status_bits_rounded desc nulls last) as status_bits_rounded_rank,
        count(humidity) over (partition by device_code order by cast(reading_ts as bigint) asc range between 3600 preceding and current row) as humidity_count_1h,
        dense_rank() over w_running as humidity_dense_rank
    from scored
    window w_grp as (partition by device_code order by humidity_share desc), w_running as (partition by device_code order by humidity desc nulls first)
),
scored_device_readings_2 as (
    -- Running and moving aggregates
    select
        reading_ts,
        status_bits_rounded,
        humidity_share_dense_rank,
        humidity_count_1h,
        humidity_dense_rank,
        dense_rank() over (order by humidity_share asc nulls last) as humidity_share_dense_rank_2,
        dense_rank() over (order by humidity asc nulls last) as humidity_dense_rank_2
    from scored_device_readings
    where humidity >= 42
)
select
    reading_ts,
    status_bits_rounded,
    humidity_share_dense_rank,
    humidity_count_1h,
    humidity_dense_rank,
    humidity_share_dense_rank_2,
    humidity_dense_rank_2
from scored_device_readings_2
order by humidity_share_dense_rank_2 asc;
