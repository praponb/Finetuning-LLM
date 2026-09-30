select *
from (
    select plan, round(stddev(monthly_fee), 2) stddev_monthly_fee
    from subscriptions
    group by plan
) s
where stddev_monthly_fee > 35
order by stddev_monthly_fee desc, plan
limit 5;
