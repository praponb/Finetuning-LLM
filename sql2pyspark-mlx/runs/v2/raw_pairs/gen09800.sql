select
    plan,
    count(*) as row_count,
    sum(case when plan = 'enterprise' then 1 else 0 end) as enterprise_count,
    sum(monthly_fee) as total_monthly_fee
from subscriptions
group by 1;
