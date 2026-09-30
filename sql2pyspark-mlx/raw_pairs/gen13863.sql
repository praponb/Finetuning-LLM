with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        key,
        value
    from app_logs
    lateral view explode(context) x as key, value
),
metrics as (
    -- Ranking and gap features
    select
        id,
        key,
        first_value(key) ignore nulls over (partition by key, service order by log_ts, id, key rows between unbounded preceding and current row) as first_known_key,
        last_value(key, true) over (partition by date_trunc('month', log_ts), service order by log_ts, id, key rows between unbounded preceding and current row) as last_known_key,
        row_number() over (order by log_ts desc, id, key) as seq_no
    from exploded
),
metrics_app_logs as (
    -- Window metrics over several different partitions
    select
        *,
        lead(seq_no) over (partition by key order by seq_no, id, key) as next_seq_no,
        lag(seq_no) over (partition by key order by seq_no, id, key) as prev_seq_no,
        row_number() over (partition by key order by seq_no, id, key) as row_seq,
        row_number() over (partition by key order by seq_no desc, id, key) as event_seq
    from metrics
),
metrics_app_logs_2 as (
    -- Window metrics over several different partitions
    select
        *,
        lag(row_seq) over (partition by key order by next_seq_no, id, key) as prev_row_seq,
        lead(next_seq_no) over (partition by key order by next_seq_no, id, key) as next_next_seq_no,
        row_number() over (partition by key order by next_seq_no, id, key) as row_seq_2,
        dense_rank() over (partition by key order by event_seq desc) as event_seq_dense_rank
    from metrics_app_logs
    where key in ('version', 'region', 'trace')
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        key,
        seq_no,
        next_seq_no,
        prev_seq_no,
        row_seq,
        event_seq,
        next_next_seq_no,
        row_seq_2,
        event_seq_dense_rank,
        cast(row_seq_2 as double) as row_seq_2_str,
        round(prev_row_seq / event_seq_dense_rank, 2) as prev_row_seq_per_event_seq_dense_rank,
        coalesce(key, 'OTHER') as key_clean
    from metrics_app_logs_2
    where key is not null
)
-- Grouped metrics
select
    key,
    percentile_approx(row_seq_2_str, array(0.25, 0.5, 0.75)) as row_seq_2_str_quartiles,
    count(case when key = 'trace' then 1 end) as trace_count
from base
group by key
order by trace_count nulls last;
