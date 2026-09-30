with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) t as attr_key, value
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by attr_key order by status_code desc, id, attr_key) as rn
        from exploded
    ) t
    where rn = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        corr(latency_ms, status_code) over (partition by attr_key) as corr_latency_ms_status_code,
        lead(status_code, 1) over w_grp as next_status_code,
        ntile(10) over (order by latency_ms desc, id, attr_key) as latency_ms_bucket,
        last_value(attr_key, true) over (partition by attr_key order by latency_ms, id, attr_key rows between unbounded preceding and current row) as last_known_attr_key,
        first_value(latency_ms) ignore nulls over (order by rn, id, attr_key rows between unbounded preceding and current row) as first_known_latency_ms
    from deduped
    window w_grp as (partition by attr_key order by status_code, id, attr_key)
),
scored_app_logs as (
    -- Running and moving aggregates
    select
        *,
        corr(latency_ms, status_code) over (partition by attr_key) as corr_latency_ms_status_code_2,
        lead(next_status_code, 1) over w_ordered as next_next_status_code,
        cume_dist() over w_hist as first_known_latency_ms_cume_dist
    from scored
    window w_ordered as (partition by attr_key order by corr_latency_ms_status_code, id, attr_key), w_hist as (partition by attr_key order by first_known_latency_ms desc nulls last)
)
select
    id,
    attr_key,
    value,
    rn,
    next_status_code,
    last_known_attr_key,
    first_known_latency_ms,
    corr_latency_ms_status_code_2,
    next_next_status_code,
    case when attr_key rlike '^(region|version)$' then 'PRIORITY' else 'CORE' end as attr_key_zone
from scored_app_logs
order by next_status_code nulls first;
