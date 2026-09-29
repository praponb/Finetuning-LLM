select
    status,
    case when monthly_fee >= 212 then 'high' when monthly_fee >= 36 then 'mid' else 'low' end as monthly_fee_bucket,
    max(start_date) as last_start_date,
    sort_array(collect_set(plan)) as plan_list
from subscriptions
group by 1, 2
order by last_start_date, monthly_fee_bucket;
