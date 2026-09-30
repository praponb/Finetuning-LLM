with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) li as attr_key, v
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(latency_ms) over (partition by attr_key order by cast(log_ts as bigint) asc range between 7200 preceding and current row) as latency_ms_avg_2h,
        round(latency_ms / sum(latency_ms) over (partition by attr_key, host), 4) as latency_ms_share
    from lines
),
with_windows as (
    -- Window metrics over several different partitions
    select
        host,
        log_ts,
        latency_ms,
        v,
        round(latency_ms / sum(latency_ms) over (partition by host), 4) as latency_ms_share_2,
        dense_rank() over (order by latency_ms desc nulls first) as latency_ms_dense_rank
    from scored
),
base as (
    -- Keep recent, valid rows
    select
        *,
        round(latency_ms_dense_rank, 1) as latency_ms_dense_rank_rounded,
        cast(latency_ms_share_2 as int) as latency_ms_share_2_int
    from with_windows
    where log_ts > timestamp '2025-06-01 11:00:00'
),
stats as (
    -- Multi-level aggregation
    select
        case when latency_ms_dense_rank >= 9 then 'PREMIUM' else 'STANDARD' end as latency_ms_dense_rank_tier,
        host,
        count(*) filter (where host = 'h1') as h1_rows,
        count_if(host = 'h3') as h3_events,
        size(collect_set(host)) as num_hosts,
        round(avg(latency_ms_share_2_int), 2) as avg_latency_ms_share_2_int,
        count_if(host = 'h1') as h1_events,
        grouping(case when latency_ms_dense_rank >= 9 then 'PREMIUM' else 'STANDARD' end) as grouping_level
    from base
    group by cube (case when latency_ms_dense_rank >= 9 then 'PREMIUM' else 'STANDARD' end, host)
    having count_if(host = 'h3') < 1000
)
select
    latency_ms_dense_rank_tier,
    host,
    h3_events,
    avg_latency_ms_share_2_int,
    h1_events,
    case when host rlike '^(h1|h3)$' then 'EXTENDED' when host rlike '^(h2|h4)$' then 'CORE' else 'PRIORITY' end as host_zone
from stats
order by avg_latency_ms_share_2_int asc
