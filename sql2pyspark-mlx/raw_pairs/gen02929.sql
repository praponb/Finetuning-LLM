with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) t as key, v
),
metrics as (
    -- Ranking and gap features
    select
        id,
        service,
        key,
        v,
        round(latency_ms / sum(latency_ms) over (partition by host), 4) as latency_ms_share,
        row_number() over (order by log_ts desc, id, key) as seq_no
    from exploded
),
scored as (
    -- Running and moving aggregates
    select
        id,
        key,
        v,
        latency_ms_share,
        rank() over (order by seq_no desc) as seq_no_rank,
        avg(seq_no) over (partition by key order by latency_ms_share, id, key rows between 5 preceding and 1 preceding) as seq_no_avg_5_prior,
        first_value(seq_no) ignore nulls over (partition by service order by latency_ms_share, id, key rows between unbounded preceding and current row) as first_known_seq_no,
        round(seq_no / sum(seq_no) over (partition by key), 4) as seq_no_share,
        ntile(4) over (partition by service order by latency_ms_share desc, id, key) as latency_ms_share_bucket
    from metrics
    where service = 'billing'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(3) over (partition by key order by seq_no_share, id, key) as seq_no_share_bucket,
        sum(seq_no_rank) over (partition by key order by seq_no_avg_5_prior, id, key rows between unbounded preceding and current row) as cumulative_seq_no_rank,
        sum(seq_no_share) over (partition by key order by seq_no_avg_5_prior, id, key rows between unbounded preceding and current row) as running_seq_no_share
    from scored
)
-- Grouped metrics
select
    key,
    round(avg(running_seq_no_share), 2) as avg_running_seq_no_share,
    size(collect_set(key)) as num_keys,
    approx_percentile(latency_ms_share_bucket, array(0.25, 0.5, 0.75)) as latency_ms_share_bucket_quartiles,
    sort_array(collect_set(key)) as key_list,
    avg(running_seq_no_share) as avg_running_seq_no_share_2
from with_windows
group by cube (key)
order by key nulls last
