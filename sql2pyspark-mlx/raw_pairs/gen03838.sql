with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view explode(context) e as key, attr_value
),
scored as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by key, host order by latency_ms nulls last) as latency_ms_dense_rank,
        lead(latency_ms, 2) over (order by latency_ms, id, key) as next_latency_ms,
        lead(status_code, 2) over (partition by key, host order by latency_ms, id, key) as next_status_code,
        avg(latency_ms) over (partition by host order by cast(log_ts as bigint) range between 3600 preceding and current row) as latency_ms_avg_1h,
        sum(latency_ms) over (partition by host order by log_ts, id, key rows between unbounded preceding and current row) as cumulative_latency_ms
    from lines
),
base as (
    -- Keep recent, valid rows
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        latency_ms_dense_rank,
        next_latency_ms,
        latency_ms_avg_1h,
        upper(key) as key_upper,
        coalesce(key, 'NONE') as key_clean
    from scored
    where key is not null
),
enriched as (
    -- Ranking and gap features
    select
        id,
        host,
        latency_ms,
        key,
        next_latency_ms,
        latency_ms_avg_1h,
        key_clean,
        lead(latency_ms_avg_1h) over w_ordered as next_latency_ms_avg_1h,
        lag(status_code) over w_ordered as prev_status_code,
        row_number() over w_ordered as event_seq,
        ntile(4) over (partition by host order by latency_ms_dense_rank desc, id, key) as latency_ms_dense_rank_bucket
    from base
    window w_ordered as (partition by host order by latency_ms, id, key)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(event_seq) over (partition by host order by latency_ms, id, key rows between 2 preceding and current row) as last3_sum_event_seq,
        max(event_seq) over (partition by host order by latency_ms, id, key rows between 2 preceding and current row) as last3_max_event_seq,
        avg(event_seq) over (partition by host order by latency_ms, id, key rows between 2 preceding and current row) as last3_avg_event_seq,
        last_value(next_latency_ms, true) over (partition by key order by latency_ms_avg_1h, id, key rows between unbounded preceding and current row) as last_known_next_latency_ms
    from enriched
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by key order by last3_avg_event_seq desc nulls last, id, key) as latest_rank
    from metrics
    qualify latest_rank = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        host,
        key,
        key_clean,
        next_latency_ms_avg_1h,
        event_seq,
        last3_sum_event_seq,
        last3_max_event_seq,
        last_known_next_latency_ms,
        concat(key, ', ', host) as key_host,
        coalesce(key, 'NONE') as key_clean_2
    from latest
    where key is not null
)
select
    id,
    host,
    key,
    next_latency_ms_avg_1h,
    last3_max_event_seq,
    key_host,
    key_clean_2,
    case when key rlike '^(region|trace)$' then 'PRIORITY' when key rlike '^(version)$' then 'GROUP_A' else 'CORE' end as key_zone
from valid
order by next_latency_ms_avg_1h nulls first;
