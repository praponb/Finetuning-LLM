with flattened as (
    select
        id,
        service,
        latency_ms,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
),
filtered as (
    select *, upper(service) as service_upper
    from flattened
    where latency_ms >= 2002
),
cleaned as (
    select
        id,
        service,
        latency_ms,
        v,
        length(attr_key) as attr_key_length,
        upper(attr_key) as attr_key_upper,
        coalesce(attr_key, 'NONE') as resolved_attr_key
    from filtered
    where attr_key is not null
),
valid as (
    select
        service,
        latency_ms,
        attr_key_length,
        resolved_attr_key,
        case when attr_key_length >= 82 then 'High' when attr_key_length >= 30 then 'Medium' else 'Low' end as attr_key_length_tier
    from cleaned
    where service in ('billing', 'checkout')
),
with_windows as (
    select
        service,
        latency_ms,
        dense_rank() over (partition by service order by latency_ms desc nulls last) as latency_ms_dense_rank,
        round(latency_ms / sum(latency_ms) over (partition by service), 4) as latency_ms_share
    from valid
    where latency_ms between 850 and 2246
),
scored as (
    select
        service,
        latency_ms,
        round(latency_ms / sum(latency_ms) over (partition by service), 4) as latency_ms_share_2,
        percent_rank() over (order by latency_ms_dense_rank desc nulls first) as latency_ms_dense_rank_percent_rank
    from with_windows
),
remaining_keys as (
    select service
    from scored
    intersect all
    select service
    from app_logs
    where latency_ms between 1195 and 1978
),
reconciled as (
    select s.*
    from scored s
    inner join remaining_keys k on s.service = k.service
),
stats as (
    select
        case when latency_ms >= 2228 then 'HIGH' else 'LOW' end as latency_ms_tier,
        bit_or(latency_ms) as latency_ms_bit_or,
        size(collect_set(service)) as num_services,
        sum(case when service = 'auth' then latency_ms_dense_rank_percent_rank else 0 end) as auth_latency_ms_dense_rank_percent_rank,
        count_if(service = 'auth') as auth_events
    from reconciled
    group by case when latency_ms >= 2228 then 'HIGH' else 'LOW' end
    having auth_events > 1
)
select latency_ms_tier, latency_ms_bit_or
from stats
