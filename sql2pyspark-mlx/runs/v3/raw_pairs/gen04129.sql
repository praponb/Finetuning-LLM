select
    plan,
    count(monthly_fee) as monthly_fee_count,
    count(case when plan = 'standard' then 1 end) as standard_count,
    round(stddev(monthly_fee), 2) as stddev_monthly_fee
from subscriptions
where plan = 'basic' and start_date < '2023-12-01'
group by plan
having sum(monthly_fee) >= 1190
order by stddev_monthly_fee desc;
