with lines as (
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) t as k, value
),
flattened_attrs as (
    select
        id,
        host,
        latency_ms,
        k,
        value,
        cast((status_code & 8) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from lines
),
cleaned as (
    select
        id,
        host,
        latency_ms,
        k,
        is_flagged,
        status_code_high,
        cast(status_code_high as string) as status_code_high_num
    from flattened_attrs
    where k is not null
),
deduped as (
    select *, row_number() over (partition by host, k order by is_flagged desc, id asc, k asc) as dedup_rank
    from cleaned
    qualify dedup_rank = 1
),
deduped_app_logs as (
    select *
    from deduped
    qualify row_number() over (partition by host, k order by is_flagged desc nulls last, id asc, k asc) = 1
),
matched_keys as (
    select host
    from deduped_app_logs
    intersect
    select host
    from app_logs
    where latency_ms = 997
),
verified as (
    select s.*
    from deduped_app_logs s
    inner join matched_keys k on s.host = k.host
),
scored as (
    select
        id,
        host,
        k,
        is_flagged,
        status_code_high_num,
        dedup_rank,
        sum(status_code_high) over w_hist as host_sum_status_code_high,
        round(latency_ms / sum(latency_ms) over (partition by k), 4) as latency_ms_share,
        avg(is_flagged) over w_hist as host_avg_is_flagged
    from verified
    window w_hist as (partition by host)
)
select
    id,
    host,
    k,
    dedup_rank,
    latency_ms_share,
    case when k rlike '^(trace|version)$' then 'OTHER_ZONE' when k rlike '^(region)$' then 'CORE' else 'GROUP_A' end as k_zone
from scored;
