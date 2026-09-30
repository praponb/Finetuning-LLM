with flattened as (
    select
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
),
with_windows as (
    select
        id,
        service,
        status_code,
        round(status_code / sum(status_code) over (partition by service, attr_key), 4) as status_code_share,
        round(status_code / sum(status_code) over (partition by service), 4) as status_code_share_2,
        cume_dist() over (order by status_code desc) as status_code_cume_dist
    from flattened
    where service in ('search', 'billing', 'checkout')
),
metrics as (
    select
        status_code,
        status_code_share,
        status_code_cume_dist,
        avg(status_code_share) over (partition by service) as service_avg_status_code_share
    from with_windows
),
cleaned as (
    select *, round(status_code_cume_dist / status_code, 2) as status_code_cume_dist_per_status_code
    from metrics
    where status_code_share is not null
),
with_windows_app_logs as (
    select
        status_code_share,
        status_code_cume_dist,
        service_avg_status_code_share,
        cume_dist() over (order by status_code_cume_dist_per_status_code desc) as status_code_cume_dist_per_status_code_cume_dist,
        dense_rank() over (order by service_avg_status_code_share nulls first) as service_avg_status_code_share_dense_rank
    from cleaned
)
select
    case when service_avg_status_code_share_dense_rank >= 8 then 'LARGE' else 'SMALL' end as service_avg_status_code_share_dense_rank_tier,
    count(*) as num_rows,
    approx_percentile(status_code_share, 0.9) as status_code_share_p90
from with_windows_app_logs
group by rollup (case when service_avg_status_code_share_dense_rank >= 8 then 'LARGE' else 'SMALL' end)
having count(*) > 1;
