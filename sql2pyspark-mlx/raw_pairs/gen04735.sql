select plan, active_count, avg_monthly_fee
from (
    select plan, count(case when status = 'ACTIVE' then 1 end) as active_count, avg(monthly_fee) as avg_monthly_fee
    from subscriptions
    group by plan
) as x
where avg_monthly_fee > 120;
