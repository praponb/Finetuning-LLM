with exploded as (
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) t as attr_key, value
),
enriched as (
    select
        *,
        avg(status_code) over (partition by attr_key) as attr_key_avg_status_code,
        dense_rank() over (order by status_code desc) as status_code_dense_rank
    from exploded
),
filtered as (
    select
        *,
        coalesce(latency_ms, 0) as latency_ms_filled,
        coalesce(attr_key, 'UNKNOWN') as attr_key_filled
    from enriched
    where log_ts >= timestamp '2025-06-01 12:00:00'
      and attr_key is not null
),
daily as (
    select
        case when status_code >= 408 then 'LARGE' else 'SMALL' end as status_code_tier,
        corr(latency_ms, status_code) as corr_latency_ms_status_code,
        round(avg(attr_key_avg_status_code), 2) as avg_attr_key_avg_status_code
    from filtered
    group by case when status_code >= 408 then 'LARGE' else 'SMALL' end
)
select
    status_code_tier,
    corr_latency_ms_status_code,
    avg_attr_key_avg_status_code
from daily
order by avg_attr_key_avg_status_code desc;
