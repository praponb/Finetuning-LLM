select
    case when monthly_fee >= 186 then 'PREMIUM' else 'STANDARD' end as monthly_fee_tier,
    count(distinct customer_id) as distinct_customer_ids,
    round(avg(monthly_fee), 2) as avg_monthly_fee,
    count(case when status = 'CANCELLED' then 1 end) as cancelled_count,
    approx_percentile(monthly_fee, array(0.25, 0.5, 0.75)) as monthly_fee_quartiles,
    count(distinct plan) as distinct_plans
from subscriptions
group by case when monthly_fee >= 186 then 'PREMIUM' else 'STANDARD' end
