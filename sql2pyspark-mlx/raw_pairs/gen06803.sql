with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        value
    from app_logs
    lateral view outer explode(context) e as k, value
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, cast(latency_ms as string) as latency_ms_num
    from exploded
    where host not in ('h4', 'h2', 'h1')
),
cleaned as (
    -- Basic cleansing and defaults
    select *, concat_ws('-', host, service) as host_service_key
    from valid
    where log_ts > date '2025-06-01'
      and k is not null
),
enriched as (
    -- Ranking and gap features
    select
        id,
        host,
        latency_ms,
        latency_ms_num,
        rank() over (partition by host, service order by latency_ms) as latency_ms_rank,
        round(latency_ms / sum(latency_ms) over (partition by k), 4) as latency_ms_share
    from cleaned
),
scored as (
    -- Running and moving aggregates
    select
        host,
        latency_ms_rank,
        percent_rank() over w_recent as latency_ms_share_percent_rank,
        percent_rank() over w_recent as latency_ms_share_percent_rank_2,
        max(latency_ms) over w_part as host_max_latency_ms,
        cume_dist() over w_hist as latency_ms_rank_cume_dist
    from enriched
    window w_recent as (order by latency_ms_share nulls last), w_part as (partition by host), w_hist as (order by latency_ms_rank desc nulls first)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        host,
        latency_ms_rank,
        latency_ms_share_percent_rank,
        latency_ms_share_percent_rank_2,
        host_max_latency_ms,
        latency_ms_rank_cume_dist,
        round(latency_ms_share_percent_rank * 0.85, 2) as latency_ms_share_percent_rank_scaled,
        cast(latency_ms_share_percent_rank_2 as int) as latency_ms_share_percent_rank_2_int
    from scored
    where latency_ms_share_percent_rank <= 0.68
)
-- Aggregate with conditional metrics
select
    host,
    case when latency_ms_rank >= 7 then 'PREMIUM' else 'STANDARD' end as latency_ms_rank_tier,
    count(distinct host) as distinct_hosts,
    round(avg(latency_ms_rank), 2) as avg_latency_ms_rank,
    sort_array(collect_set(host)) as host_list,
    max(latency_ms_rank_cume_dist) filter (where host = 'h1') as h1_max_latency_ms_rank_cume_dist,
    count(*) as cnt,
    grouping_id() as grouping_id
from filtered
group by grouping sets ((host, case when latency_ms_rank >= 7 then 'PREMIUM' else 'STANDARD' end), (host), ())
having round(avg(latency_ms_rank), 2) > 0
order by avg_latency_ms_rank desc nulls first;
