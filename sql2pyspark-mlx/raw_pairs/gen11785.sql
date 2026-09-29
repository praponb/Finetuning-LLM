with valid as (
    -- Basic cleansing and defaults
    select *, coalesce(humidity, 0) as humidity_filled
    from device_readings
    where reading_ts > date '2025-09-01'
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by device_code order by humidity_filled desc nulls last, id asc) as dedup_rank
        from valid
    ) t
    where dedup_rank = 1
),
ranked_device_readings as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        firmware,
        dedup_rank
    from (
        select *, row_number() over (partition by device_code order by temp_c desc, id asc) as latest_rank
        from ranked
    ) x
    where latest_rank = 1
),
with_prev as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) as prev_reading_ts
    from ranked_device_readings
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '30' minute then 1 else 0 end as is_new_burst
    from with_prev
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        dedup_rank,
        sum(is_new_burst) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) > interval 20 minutes then 1 else 0 end as is_new_session
    from sessions_device_readings
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as island_id
    from flagged
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        is_new_session,
        min(burst_id) over (partition by device_code order by dedup_rank asc, id asc rows between 2 preceding and current row) as last3_min_burst_id,
        avg(burst_id) over (partition by device_code order by dedup_rank asc, id asc rows between 2 preceding and current row) as last3_avg_burst_id,
        count(burst_id) over (partition by device_code order by dedup_rank asc, id asc rows between 2 preceding and current row) as last3_count_burst_id,
        rank() over (partition by device_code order by island_id desc) as island_id_rank
    from sessionized
    where burst_id < 7
),
ranked_device_readings_2 as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by device_code order by last3_avg_burst_id desc, id asc) as latest_rank
        from with_windows
    ) x
    where latest_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        device_code,
        last3_avg_burst_id,
        temp_c,
        last3_min_burst_id
    from ranked_device_readings_2
) src
unpivot (
    reading for metric_name in (last3_avg_burst_id, temp_c, last3_min_burst_id)
) unp;
