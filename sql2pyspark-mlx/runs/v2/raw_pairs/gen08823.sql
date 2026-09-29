select
    plan,
    status,
    count(case when plan = 'standard' then 1 end) as standard_count,
    sum(case when plan = 'premium' then monthly_fee else 0 end) as premium_monthly_fee
from subscriptions
where monthly_fee >= 110
group by plan, status
order by status
