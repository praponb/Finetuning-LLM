select *
from (
    select quarter(end_date) end_quarter, count(distinct monthly_fee) unique_plans
    from subscriptions
    group by quarter(end_date)
) t
where unique_plans > 7
order by end_quarter desc
limit 10;
