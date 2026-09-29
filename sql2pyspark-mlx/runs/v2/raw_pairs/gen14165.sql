select plan, avg_monthly_fee
from (
    select plan, round(avg(monthly_fee), 2) avg_monthly_fee
    from subscriptions
    where year(start_date) = 2025
    group by plan
) x
where avg_monthly_fee >= 62;
