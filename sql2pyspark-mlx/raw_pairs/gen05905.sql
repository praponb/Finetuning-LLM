with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) li as k, v
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        host,
        status_code,
        k,
        first_value(latency_ms) ignore nulls over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as first_known_latency_ms,
        row_number() over (partition by k order by status_code, id, k) as row_seq
    from flattened
),
with_windows_app_logs as (
    -- Ranking and gap features
    select
        id,
        status_code,
        k,
        first_known_latency_ms,
        row_seq,
        min(row_seq) over (partition by host order by row_seq, id, k rows between 3 preceding and current row) as last4_min_row_seq,
        count(row_seq) over (partition by host order by row_seq, id, k rows between 3 preceding and current row) as last4_count_row_seq,
        sum(row_seq) over (partition by host order by row_seq, id, k rows between 3 preceding and current row) as last4_sum_row_seq,
        row_number() over (partition by host order by first_known_latency_ms desc, id, k) as row_seq_2
    from with_windows
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        status_code,
        k,
        first_known_latency_ms,
        row_seq,
        last4_min_row_seq,
        last4_count_row_seq,
        last4_sum_row_seq,
        row_seq_2,
        sum(last4_min_row_seq) over (partition by k order by row_seq, id, k rows between 6 preceding and current row) as last7_sum_last4_min_row_seq,
        max(last4_min_row_seq) over (partition by k order by row_seq, id, k rows between 6 preceding and current row) as last7_max_last4_min_row_seq,
        min(last4_min_row_seq) over (partition by k order by row_seq, id, k rows between 6 preceding and current row) as last7_min_last4_min_row_seq
    from with_windows_app_logs
)
-- Multi-level aggregation
select
    k,
    sort_array(collect_set(k)) as k_list,
    round(sum(last4_sum_row_seq) / nullif(count(case when k = 'version' then 1 end), 0), 2) as last4_sum_row_seq_per_version,
    bit_and(status_code) as status_code_bit_and,
    avg(last4_count_row_seq) filter (where k = 'region') as region_avg_last4_count_row_seq
from enriched
group by k
having status_code_bit_and >= 1
order by region_avg_last4_count_row_seq desc
