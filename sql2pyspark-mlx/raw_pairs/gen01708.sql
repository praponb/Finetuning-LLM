with prev_events as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) as prev_reading_ts
    from device_readings
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 1 hour then 1 else 0 end as is_new_burst
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as burst_id
    from gaps
),
enriched as (
    -- Ranking and gap features
    select
        id,
        device_code,
        reading_ts,
        pressure,
        prev_reading_ts,
        burst_id,
        max(is_new_burst) over w_hist is_new_burst_max_15m,
        row_number() over w_recent seq_no,
        cume_dist() over (partition by device_code order by humidity desc nulls last) humidity_cume_dist,
        count(burst_id) over (partition by device_code order by cast(reading_ts as bigint) asc range between 1800 preceding and current row) burst_id_count_30m,
        ntile(5) over (order by burst_id desc, id asc) burst_id_bucket
    from islands
    window w_hist as (partition by device_code order by cast(reading_ts as bigint) asc range between 900 preceding and current row), w_recent as (partition by device_code order by temp_c asc, id asc)
),
enriched_device_readings as (
    -- Window metrics over several different partitions
    select
        *,
        sum(is_new_burst_max_15m) over (partition by device_code order by cast(prev_reading_ts as bigint) asc range between 604800 preceding and current row) is_new_burst_max_15m_sum_7d,
        cume_dist() over (partition by device_code order by is_new_burst_max_15m asc nulls last) is_new_burst_max_15m_cume_dist,
        ntile(5) over (partition by device_code order by seq_no asc, id asc) seq_no_bucket,
        sum(burst_id) over (partition by device_code order by burst_id asc, id asc rows between unbounded preceding and current row) cumulative_burst_id,
        dense_rank() over (partition by device_code order by humidity_cume_dist desc) humidity_cume_dist_dense_rank
    from enriched
),
metrics as (
    -- Ranking and gap features
    select
        id,
        reading_ts,
        pressure,
        humidity_cume_dist,
        burst_id_count_30m,
        burst_id_bucket,
        is_new_burst_max_15m_sum_7d,
        seq_no_bucket,
        cumulative_burst_id,
        humidity_cume_dist_dense_rank,
        sum(humidity_cume_dist) over w_ordered running_sum_humidity_cume_dist,
        max(humidity_cume_dist) over w_ordered running_max_humidity_cume_dist,
        avg(humidity_cume_dist) over w_ordered running_avg_humidity_cume_dist,
        count(humidity_cume_dist) over w_ordered running_count_humidity_cume_dist
    from enriched_device_readings
    window w_ordered as (partition by device_code order by pressure asc, id asc rows between unbounded preceding and current row)
),
stats as (
    -- Grouped metrics
    select
        case when humidity_cume_dist >= 0.88 then 'PREMIUM' else 'STANDARD' end as humidity_cume_dist_tier,
        cumulative_burst_id,
        case when running_avg_humidity_cume_dist >= 0.68 then 'HIGH' else 'LOW' end as running_avg_humidity_cume_dist_tier,
        count(*) as row_count,
        percentile_approx(burst_id_bucket, 0.95) as burst_id_bucket_p95,
        approx_percentile(pressure, 0.95) as pressure_p95
    from metrics
    group by grouping sets ((case when humidity_cume_dist >= 0.88 then 'PREMIUM' else 'STANDARD' end, cumulative_burst_id, case when running_avg_humidity_cume_dist >= 0.68 then 'HIGH' else 'LOW' end), (case when humidity_cume_dist >= 0.88 then 'PREMIUM' else 'STANDARD' end, cumulative_burst_id), ())
    having burst_id_bucket_p95 >= 0
)
select
    humidity_cume_dist_tier,
    running_avg_humidity_cume_dist_tier,
    burst_id_bucket_p95,
    pressure_p95
from stats
order by pressure_p95 desc nulls first, burst_id_bucket_p95 desc;
