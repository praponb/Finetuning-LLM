with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) x as attr_key, value
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(status_code) over (partition by service order by cast(log_ts as bigint) range between 900 preceding and current row) as status_code_sum_15m,
        rank() over w_part as status_code_rank,
        max(latency_ms) over w_part_2 as level_max_latency_ms,
        round(status_code / sum(status_code) over (partition by service), 4) as status_code_share,
        corr(status_code, latency_ms) over w_hist as corr_status_code_latency_ms
    from exploded
    where latency_ms <= 790
    window w_part as (partition by attr_key order by status_code desc), w_part_2 as (partition by level), w_hist as (partition by service)
),
matched_keys as (
    -- Set intersection on keys
    select id
    from enriched
    intersect all
    select id
    from app_logs
    where status_code = 441
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.id = k.id
),
with_windows as (
    -- Ranking and gap features
    select
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        value,
        status_code_sum_15m,
        status_code_share,
        corr_status_code_latency_ms,
        percent_rank() over (partition by to_date(log_ts), attr_key order by level_max_latency_ms nulls last) as level_max_latency_ms_percent_rank
    from reconciled
),
scored as (
    -- Ranking and gap features
    select
        service,
        level,
        latency_ms,
        status_code,
        status_code_sum_15m,
        status_code_share,
        corr_status_code_latency_ms,
        level_max_latency_ms_percent_rank,
        cume_dist() over (order by status_code desc) as status_code_cume_dist,
        dense_rank() over (order by level_max_latency_ms_percent_rank desc nulls first) as level_max_latency_ms_percent_rank_dense_rank
    from with_windows
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, corr_status_code_latency_ms * status_code_share as corr_status_code_latency_ms_times_status_code_share
    from scored
    where level not in ('ERROR', 'DEBUG')
),
enriched_app_logs as (
    -- Running and moving aggregates
    select
        service,
        latency_ms,
        status_code_sum_15m,
        status_code_share,
        level_max_latency_ms_percent_rank,
        level_max_latency_ms_percent_rank_dense_rank,
        cume_dist() over (order by status_code_cume_dist desc) as status_code_cume_dist_cume_dist,
        max(status_code) over w_running as level_max_status_code,
        dense_rank() over (partition by service order by latency_ms nulls first) as latency_ms_dense_rank
    from cleaned
    window w_running as (partition by level)
),
daily as (
    -- Multi-level aggregation
    select
        case when level_max_latency_ms_percent_rank_dense_rank >= 7 then 'HIGH' else 'LOW' end as level_max_latency_ms_percent_rank_dense_rank_tier,
        service,
        sum(case when service = 'auth' then status_code_sum_15m else 0 end) as auth_status_code_sum_15m,
        round(sum(status_code_sum_15m) / nullif(count(case when service = 'checkout' then 1 end), 0), 2) as status_code_sum_15m_per_checkout,
        count(case when service = 'billing' then 1 end) as billing_count,
        bit_or(latency_ms) as latency_ms_bit_or,
        sum(case when service = 'checkout' then latency_ms_dense_rank else 0 end) as checkout_latency_ms_dense_rank
    from enriched_app_logs
    group by case when level_max_latency_ms_percent_rank_dense_rank >= 7 then 'HIGH' else 'LOW' end, service
    having bit_or(latency_ms) > 1
)
select
    level_max_latency_ms_percent_rank_dense_rank_tier,
    service,
    status_code_sum_15m_per_checkout,
    latency_ms_bit_or
from daily
order by status_code_sum_15m_per_checkout desc, latency_ms_bit_or desc
