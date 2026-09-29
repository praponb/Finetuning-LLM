select *
from (
    select plan, avg(monthly_fee) avg_monthly_fee
    from subscriptions
    where plan != 'standard'
    group by plan
) s
where avg_monthly_fee >= 180
order by avg_monthly_fee;
