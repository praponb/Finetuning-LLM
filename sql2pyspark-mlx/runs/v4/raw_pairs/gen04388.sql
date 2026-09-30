select
    case when plan in ('premium', 'basic') then 'core' else 'other' end as plan_group,
    sum(monthly_fee) as total_monthly_fee,
    count(*) as cnt,
    sum(sum(monthly_fee)) over (order by case when plan in ('premium', 'basic') then 'core' else 'other' end) as cumulative_monthly_fee
from subscriptions
where monthly_fee < 160
group by case when plan in ('premium', 'basic') then 'core' else 'other' end
order by cnt desc
