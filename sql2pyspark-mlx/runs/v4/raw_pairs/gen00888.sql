with exploded as (
    select
        id,
        service,
        log_ts,
        status_code,
        key,
        v
    from app_logs
    lateral view outer explode(context) x as key, v
),
unpacked as (
    select
        id,
        service,
        cast((status_code & 8) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
filtered as (
    select *, least(status_code_high, has_fault) as least_status_code_high_has_fault
    from unpacked
    where service in ('checkout', 'search', 'billing')
),
metrics as (
    select
        *,
        sum(least_status_code_high_has_fault) over (partition by service) as service_sum_least_status_code_high_has_fault,
        dense_rank() over (partition by service order by status_code_high desc nulls first) as status_code_high_dense_rank,
        avg(least_status_code_high_has_fault) over (partition by service) as service_avg_least_status_code_high_has_fault,
        percent_rank() over (partition by service order by least_status_code_high_has_fault desc nulls first) as least_status_code_high_has_fault_percent_rank,
        cume_dist() over (partition by service order by least_status_code_high_has_fault nulls last) as least_status_code_high_has_fault_cume_dist
    from filtered
),
summary as (
    select
        case when least_status_code_high_has_fault_percent_rank >= 0.67 then 'PREMIUM' else 'STANDARD' end as least_status_code_high_has_fault_percent_rank_tier,
        service,
        count(*) as cnt,
        sort_array(collect_set(service)) as service_list,
        sum(service_sum_least_status_code_high_has_fault) as total_service_sum_least_status_code_high_has_fault,
        round(sum(status_code_high_dense_rank) / nullif(count(case when service = 'auth' then 1 end), 0), 2) as status_code_high_dense_rank_per_auth,
        sum(case when service = 'auth' then least_status_code_high_has_fault_percent_rank else 0 end) as auth_least_status_code_high_has_fault_percent_rank,
        percentile_approx(has_fault, array(0.25, 0.5, 0.75)) as has_fault_quartiles,
        grouping(case when least_status_code_high_has_fault_percent_rank >= 0.67 then 'PREMIUM' else 'STANDARD' end) as grouping_id
    from metrics
    group by rollup (case when least_status_code_high_has_fault_percent_rank >= 0.67 then 'PREMIUM' else 'STANDARD' end, service)
    having total_service_sum_least_status_code_high_has_fault < 0
)
select
    least_status_code_high_has_fault_percent_rank_tier,
    service,
    service_list,
    total_service_sum_least_status_code_high_has_fault,
    status_code_high_dense_rank_per_auth,
    auth_least_status_code_high_has_fault_percent_rank,
    has_fault_quartiles,
    grouping_id
from summary
