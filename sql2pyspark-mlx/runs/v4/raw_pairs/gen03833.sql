with enriched as (
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        max(monthly_fee) over (partition by plan, status) as plan_max_monthly_fee,
        last_value(monthly_fee, true) over w_part as last_known_monthly_fee,
        percent_rank() over w_hist as monthly_fee_percent_rank
    from subscriptions
    window w_part as (partition by customer_id order by end_date, id rows between unbounded preceding and current row), w_hist as (order by monthly_fee desc)
)
select
    case when monthly_fee_percent_rank >= 0.71 then 'HIGH' else 'LOW' end as monthly_fee_percent_rank_tier,
    plan,
    sum(case when status = 'ACTIVE' then monthly_fee_percent_rank else 0 end) as active_monthly_fee_percent_rank,
    count(*) filter (where plan = 'enterprise') as enterprise_rows,
    sort_array(collect_set(plan)) as plan_list
from enriched
group by cube (case when monthly_fee_percent_rank >= 0.71 then 'HIGH' else 'LOW' end, plan)
having active_monthly_fee_percent_rank >= 0
order by enterprise_rows desc nulls first
