select
    plan,
    start_year,
    stddev_monthly_fee,
    min_monthly_fee
from (
    select
        plan,
        year(start_date) as start_year,
        round(stddev(monthly_fee), 2) as stddev_monthly_fee,
        min(monthly_fee) as min_monthly_fee
    from subscriptions
    group by plan, year(start_date)
) as sub
where stddev_monthly_fee > 41;
